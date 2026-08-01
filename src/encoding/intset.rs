//! Intset -- a sorted array of same-width integers.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! Layout from `intset.c`: 4-byte encoding LE (2, 4 or 8), 4-byte length LE,
//! then `length` little-endian integers of that width, sorted ascending.
//! Upgrades in place when a wider value is inserted. RDB (W3a) writes it
//! verbatim.
//!
//! ```text
//! +-------------+-------------+-----------------------------+
//! | encoding:u32| length:u32  | contents[length * encoding] |
//! +-------------+-------------+-----------------------------+
//! ```
//!
//! `encoding` is the *width in bytes* of one element. Contents are sorted, so
//! `contains` is a binary search and `SRANDMEMBER`/`SPOP` get an O(1) index.
//! Little-endian on disk regardless of host endianness, for the same reason
//! listpack is: real Redis has to be able to read what we write.
//!
//! Entirely safe Rust -- the blob is a `Vec<u8>` and every read is a
//! bounds-checked `get`.

/// `INTSET_ENC_INT16`.
pub const ENC_INT16: u32 = 2;
/// `INTSET_ENC_INT32`.
pub const ENC_INT32: u32 = 4;
/// `INTSET_ENC_INT64`.
pub const ENC_INT64: u32 = 8;

const HDR: usize = 8;

/// Smallest width that can hold `v`, matching `_intsetValueEncoding`.
#[inline]
pub fn value_encoding(v: i64) -> u32 {
    if v < i64::from(i32::MIN) || v > i64::from(i32::MAX) {
        ENC_INT64
    } else if v < i64::from(i16::MIN) || v > i64::from(i16::MAX) {
        ENC_INT32
    } else {
        ENC_INT16
    }
}

/// A sorted set of integers in Redis's serialized intset format.
#[derive(Clone, PartialEq, Eq)]
pub struct Intset {
    buf: Vec<u8>,
}

impl Default for Intset {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Intset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Intset")
            .field("encoding", &self.encoding())
            .field("len", &self.len())
            .finish()
    }
}

impl Intset {
    /// An empty intset at the narrowest encoding, exactly as `intsetNew`.
    #[inline]
    pub fn new() -> Self {
        let mut buf = Vec::with_capacity(HDR);
        buf.extend_from_slice(&ENC_INT16.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        Intset { buf }
    }

    /// Build from an iterator, widening the encoding as needed.
    pub fn from_iter_i64<I: IntoIterator<Item = i64>>(items: I) -> Self {
        let mut s = Intset::new();
        for v in items {
            s.add(v);
        }
        s
    }

    /// Adopt a serialized intset, validating it first.
    ///
    /// RDB input is untrusted (§6): a bad width, a truncated body or contents
    /// that are not strictly ascending are all rejected, because `contains`
    /// binary-searches them and would otherwise silently miss.
    pub fn from_bytes(buf: Vec<u8>) -> Option<Self> {
        let h = buf.get(..HDR)?;
        let enc = u32::from_le_bytes([h[0], h[1], h[2], h[3]]);
        if enc != ENC_INT16 && enc != ENC_INT32 && enc != ENC_INT64 {
            return None;
        }
        let len = u32::from_le_bytes([h[4], h[5], h[6], h[7]]) as usize;
        let want = len.checked_mul(enc as usize)?.checked_add(HDR)?;
        if want != buf.len() {
            return None;
        }
        let s = Intset { buf };
        let mut prev: Option<i64> = None;
        for i in 0..len {
            let v = s.get(i)?;
            if let Some(p) = prev
                && v <= p
            {
                return None;
            }
            prev = Some(v);
        }
        Some(s)
    }

    /// The serialized blob, ready for RDB.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    #[inline]
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    /// Element width in bytes: 2, 4 or 8.
    #[inline]
    pub fn encoding(&self) -> u32 {
        match self.buf.get(..4) {
            Some(s) => u32::from_le_bytes([s[0], s[1], s[2], s[3]]),
            None => ENC_INT16,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self.buf.get(4..8) {
            Some(s) => u32::from_le_bytes([s[0], s[1], s[2], s[3]]) as usize,
            None => 0,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate heap footprint, for `MEMORY USAGE`.
    #[inline]
    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>() + self.buf.capacity()
    }

    #[inline]
    fn set_len(&mut self, n: usize) {
        if let Some(s) = self.buf.get_mut(4..8) {
            s.copy_from_slice(&(n as u32).to_le_bytes());
        }
    }

    #[inline]
    fn set_encoding(&mut self, enc: u32) {
        if let Some(s) = self.buf.get_mut(..4) {
            s.copy_from_slice(&enc.to_le_bytes());
        }
    }

    /// The element at `index`, or `None` when out of range.
    #[inline]
    pub fn get(&self, index: usize) -> Option<i64> {
        let enc = self.encoding() as usize;
        let at = HDR.checked_add(index.checked_mul(enc)?)?;
        let s = self.buf.get(at..at.checked_add(enc)?)?;
        Some(match enc {
            2 => i64::from(i16::from_le_bytes([s[0], s[1]])),
            4 => i64::from(i32::from_le_bytes([s[0], s[1], s[2], s[3]])),
            _ => i64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]),
        })
    }

    /// Overwrite the element at `index`. `v` must fit the current width.
    #[inline]
    fn put(&mut self, index: usize, v: i64) {
        let enc = self.encoding() as usize;
        let at = HDR + index * enc;
        let Some(s) = self.buf.get_mut(at..at + enc) else {
            return;
        };
        match enc {
            2 => s.copy_from_slice(&(v as i16).to_le_bytes()),
            4 => s.copy_from_slice(&(v as i32).to_le_bytes()),
            _ => s.copy_from_slice(&v.to_le_bytes()),
        }
    }

    /// `intsetSearch`: binary search. `Ok(index)` when present,
    /// `Err(insert_position)` otherwise.
    #[inline]
    pub fn search(&self, v: i64) -> Result<usize, usize> {
        let n = self.len();
        if n == 0 {
            return Err(0);
        }
        // Redis short-circuits the out-of-range cases before searching. The
        // pattern (monotonically increasing SADD) is common enough that this
        // skips log n probes on a very hot path.
        if self.get(n - 1).is_some_and(|max| v > max) {
            return Err(n);
        }
        if self.get(0).is_some_and(|min| v < min) {
            return Err(0);
        }
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.get(mid) {
                Some(cur) if cur < v => lo = mid + 1,
                Some(cur) if cur > v => hi = mid,
                Some(_) => return Ok(mid),
                None => return Err(lo),
            }
        }
        Err(lo)
    }

    /// `intsetFind`.
    #[inline]
    pub fn contains(&self, v: i64) -> bool {
        self.search(v).is_ok()
    }

    /// `intsetAdd`. Returns true when the value was not already present.
    pub fn add(&mut self, v: i64) -> bool {
        let need = value_encoding(v);
        if need > self.encoding() {
            // It cannot already be present: it does not fit the current width.
            self.upgrade_and_add(v, need);
            return true;
        }
        match self.search(v) {
            Ok(_) => false,
            Err(pos) => {
                self.insert_at(pos, v);
                true
            }
        }
    }

    /// `intsetRemove`. Returns true when the value was present.
    pub fn remove(&mut self, v: i64) -> bool {
        if value_encoding(v) > self.encoding() {
            return false;
        }
        let Ok(pos) = self.search(v) else {
            return false;
        };
        let enc = self.encoding() as usize;
        let n = self.len();
        let from = HDR + (pos + 1) * enc;
        let to = HDR + pos * enc;
        let end = self.buf.len();
        self.buf.copy_within(from..end, to);
        self.buf.truncate(end - enc);
        self.set_len(n - 1);
        true
    }

    /// Drop every element, keeping the allocation and the encoding (Redis
    /// never downgrades an intset's width).
    pub fn clear(&mut self) {
        self.buf.truncate(HDR);
        self.set_len(0);
    }

    /// Make room at `pos` and store `v`. `v` must fit the current width.
    fn insert_at(&mut self, pos: usize, v: i64) {
        let enc = self.encoding() as usize;
        let n = self.len();
        let old = self.buf.len();
        self.buf.resize(old + enc, 0);
        let at = HDR + pos * enc;
        self.buf.copy_within(at..old, at + enc);
        self.set_len(n + 1);
        self.put(pos, v);
    }

    /// `intsetUpgradeAndAdd`: widen every element, then place `v` at whichever
    /// end it belongs to -- it is necessarily outside the old range, since not
    /// fitting the old width is why we are upgrading at all.
    fn upgrade_and_add(&mut self, v: i64, new_enc: u32) {
        let n = self.len();
        let ne = new_enc as usize;

        let mut out = Vec::with_capacity(HDR + (n + 1) * ne);
        out.extend_from_slice(&new_enc.to_le_bytes());
        out.extend_from_slice(&((n + 1) as u32).to_le_bytes());

        let prepend = v < 0;
        if prepend {
            push_le(&mut out, v, ne);
        }
        for i in 0..n {
            push_le(&mut out, self.get(i).unwrap_or(0), ne);
        }
        if !prepend {
            push_le(&mut out, v, ne);
        }
        self.buf = out;
        self.set_encoding(new_enc);
    }

    /// Smallest element, or `None` when empty.
    #[inline]
    pub fn min(&self) -> Option<i64> {
        self.get(0)
    }

    /// Largest element, or `None` when empty.
    #[inline]
    pub fn max(&self) -> Option<i64> {
        self.get(self.len().checked_sub(1)?)
    }

    /// A uniformly random element -- `SRANDMEMBER`/`SPOP` want exactly this.
    #[inline]
    pub fn random(&self) -> Option<i64> {
        let n = self.len();
        if n == 0 {
            return None;
        }
        self.get(crate::util::rand::below(n))
    }

    /// Ascending iterator.
    #[inline]
    pub fn iter(&self) -> IntsetIter<'_> {
        IntsetIter {
            s: self,
            i: 0,
            j: self.len(),
        }
    }
}

#[inline]
fn push_le(out: &mut Vec<u8>, v: i64, width: usize) {
    match width {
        2 => out.extend_from_slice(&(v as i16).to_le_bytes()),
        4 => out.extend_from_slice(&(v as i32).to_le_bytes()),
        _ => out.extend_from_slice(&v.to_le_bytes()),
    }
}

/// Ascending (and, via `next_back`, descending) iterator over an intset.
#[derive(Clone)]
pub struct IntsetIter<'a> {
    s: &'a Intset,
    i: usize,
    j: usize,
}

impl Iterator for IntsetIter<'_> {
    type Item = i64;

    #[inline]
    fn next(&mut self) -> Option<i64> {
        if self.i >= self.j {
            return None;
        }
        let v = self.s.get(self.i)?;
        self.i += 1;
        Some(v)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.j - self.i;
        (n, Some(n))
    }
}

impl DoubleEndedIterator for IntsetIter<'_> {
    #[inline]
    fn next_back(&mut self) -> Option<i64> {
        if self.i >= self.j {
            return None;
        }
        self.j -= 1;
        self.s.get(self.j)
    }
}

impl ExactSizeIterator for IntsetIter<'_> {}

impl<'a> IntoIterator for &'a Intset {
    type Item = i64;
    type IntoIter = IntsetIter<'a>;
    #[inline]
    fn into_iter(self) -> IntsetIter<'a> {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_layout_matches_redis() {
        let s = Intset::new();
        assert_eq!(s.as_bytes(), &[2, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(s.encoding(), ENC_INT16);
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
        assert!(s.min().is_none() && s.max().is_none() && s.random().is_none());
    }

    #[test]
    fn add_keeps_sorted_and_dedups() {
        let mut s = Intset::new();
        for v in [5i64, 1, 3, 1, 5, -2] {
            s.add(v);
        }
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![-2, 1, 3, 5]);
        assert_eq!(s.len(), 4);
        assert!(!s.add(3));
        assert!(s.add(4));
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![-2, 1, 3, 4, 5]);
    }

    #[test]
    fn encoding_upgrade_boundaries() {
        let mut s = Intset::new();
        s.add(1);
        assert_eq!(s.encoding(), ENC_INT16);
        s.add(i64::from(i16::MAX));
        s.add(i64::from(i16::MIN));
        assert_eq!(s.encoding(), ENC_INT16);
        assert_eq!(s.as_bytes().len(), 8 + 3 * 2);

        s.add(i64::from(i16::MAX) + 1);
        assert_eq!(s.encoding(), ENC_INT32);
        assert_eq!(s.as_bytes().len(), 8 + 4 * 4);
        assert_eq!(
            s.iter().collect::<Vec<_>>(),
            vec![
                i64::from(i16::MIN),
                1,
                i64::from(i16::MAX),
                i64::from(i16::MAX) + 1
            ]
        );

        s.add(i64::from(i32::MIN) - 1);
        assert_eq!(s.encoding(), ENC_INT64);
        assert_eq!(s.min(), Some(i64::from(i32::MIN) - 1));
        assert_eq!(s.max(), Some(i64::from(i16::MAX) + 1));
        assert_eq!(s.as_bytes().len(), 8 + 5 * 8);

        s.add(i64::MAX);
        s.add(i64::MIN);
        assert_eq!(s.encoding(), ENC_INT64);
        assert_eq!(s.min(), Some(i64::MIN));
        assert_eq!(s.max(), Some(i64::MAX));
        assert_eq!(s.len(), 7);
    }

    #[test]
    fn value_encoding_edges() {
        assert_eq!(value_encoding(0), ENC_INT16);
        assert_eq!(value_encoding(i64::from(i16::MAX)), ENC_INT16);
        assert_eq!(value_encoding(i64::from(i16::MIN)), ENC_INT16);
        assert_eq!(value_encoding(i64::from(i16::MAX) + 1), ENC_INT32);
        assert_eq!(value_encoding(i64::from(i16::MIN) - 1), ENC_INT32);
        assert_eq!(value_encoding(i64::from(i32::MAX)), ENC_INT32);
        assert_eq!(value_encoding(i64::from(i32::MAX) + 1), ENC_INT64);
        assert_eq!(value_encoding(i64::from(i32::MIN) - 1), ENC_INT64);
    }

    #[test]
    fn upgrade_prepends_negatives_and_appends_positives() {
        let mut s = Intset::from_iter_i64([1, 2, 3]);
        s.add(-100_000);
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![-100_000, 1, 2, 3]);

        let mut s = Intset::from_iter_i64([1, 2, 3]);
        s.add(100_000);
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![1, 2, 3, 100_000]);
    }

    #[test]
    fn contains_and_remove() {
        let mut s = Intset::from_iter_i64([10, 20, 30, 40]);
        assert!(s.contains(10) && s.contains(40));
        assert!(!s.contains(15) && !s.contains(0) && !s.contains(1000));
        // A value too wide for the current encoding cannot be present.
        assert!(!s.contains(i64::MAX));
        assert!(!s.remove(i64::MAX));

        assert!(s.remove(20));
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![10, 30, 40]);
        assert!(!s.remove(20));
        assert!(s.remove(10));
        assert!(s.remove(40));
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![30]);
        assert!(s.remove(30));
        assert!(s.is_empty());
        assert_eq!(s.as_bytes().len(), 8);
    }

    #[test]
    fn remove_does_not_downgrade_encoding() {
        // Redis never downgrades; the encoding is sticky.
        let mut s = Intset::from_iter_i64([1, 100_000]);
        assert_eq!(s.encoding(), ENC_INT32);
        s.remove(100_000);
        assert_eq!(s.encoding(), ENC_INT32);
        assert_eq!(s.iter().collect::<Vec<_>>(), vec![1]);
        s.clear();
        assert!(s.is_empty());
        assert_eq!(s.encoding(), ENC_INT32);
    }

    #[test]
    fn get_and_search_bounds() {
        let s = Intset::from_iter_i64([1, 3, 5]);
        assert_eq!(s.get(0), Some(1));
        assert_eq!(s.get(2), Some(5));
        assert_eq!(s.get(3), None);
        assert_eq!(s.search(1), Ok(0));
        assert_eq!(s.search(5), Ok(2));
        assert_eq!(s.search(0), Err(0));
        assert_eq!(s.search(2), Err(1));
        assert_eq!(s.search(6), Err(3));
        assert_eq!(Intset::new().search(7), Err(0));
    }

    #[test]
    fn double_ended_iteration() {
        let s = Intset::from_iter_i64([1, 2, 3, 4]);
        assert_eq!(s.iter().rev().collect::<Vec<_>>(), vec![4, 3, 2, 1]);
        assert_eq!(s.iter().len(), 4);
        let mut it = s.iter();
        assert_eq!(it.next(), Some(1));
        assert_eq!(it.next_back(), Some(4));
        assert_eq!(it.collect::<Vec<_>>(), vec![2, 3]);
    }

    #[test]
    fn from_bytes_validates() {
        let good = Intset::from_iter_i64([1, 2, 3]).into_bytes();
        assert!(Intset::from_bytes(good.clone()).is_some());

        assert!(Intset::from_bytes(vec![]).is_none());
        let mut bad = good.clone();
        bad[0] = 3; // illegal width
        assert!(Intset::from_bytes(bad).is_none());
        let mut bad = good.clone();
        bad[4] = 9; // length lies
        assert!(Intset::from_bytes(bad).is_none());
        let mut bad = good.clone();
        bad.pop(); // truncated
        assert!(Intset::from_bytes(bad).is_none());
        // Unsorted contents would break the binary search.
        let mut bad = good;
        bad[8] = 99;
        assert!(Intset::from_bytes(bad).is_none());
    }

    #[test]
    fn random_stays_in_the_set() {
        let s = Intset::from_iter_i64([-5, 0, 7, 900_000]);
        for _ in 0..200 {
            let v = s.random().expect("non-empty");
            assert!(s.contains(v));
        }
    }
}
