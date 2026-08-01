//! Quicklist -- a doubly linked list of (optionally LZF-compressed) listpacks.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! Backs lists above `list-max-listpack-size`. `list-compress-depth` controls
//! how many nodes at each end stay uncompressed; the rest go through
//! `crate::util::lzf`.
//!
//! # Node fill
//!
//! `list-max-listpack-size` (`fill`) means what it means in `quicklist.c`:
//!
//! * `fill > 0` -- at most `fill` entries per node.
//! * `fill < 0` -- a size class: -1 = 4 KB, -2 = 8 KB (the default), -3 = 16 KB,
//!   -4 = 32 KB, -5 = 64 KB.
//!
//! An element larger than the node limit becomes a *plain* node holding one
//! raw value, exactly as Redis does, so that a 1 MB list element does not
//! force a 1 MB listpack.
//!
//! # Structure
//!
//! Redis uses a hand-rolled doubly linked list. This uses a `VecDeque<Node>`,
//! deliberately:
//!
//! * push/pop at either end -- the overwhelmingly common list operation -- is
//!   O(1) either way;
//! * `index(i)` walks nodes accumulating counts in both designs, but a
//!   contiguous `VecDeque` walks them without a pointer chase per node;
//! * a middle insert costs one `memmove` of `Node` structs (40 bytes each,
//!   ~1200 nodes for a million 10-byte elements at the default 8 KB fill),
//!   which is cheaper than it sounds and far rarer than the ends.
//!
//! # Compression: not implemented, seam left open
//!
//! `list-compress-depth` is **not honoured yet**: interior nodes are never LZF
//! compressed. The seam is [`Node::data`] being an enum plus
//! [`Quicklist::compress_depth`] being stored and reported. Adding a
//! `Lzf(Box<[u8]>)` variant plus decompress-on-touch in `node_lp` /
//! `node_lp_mut` is the whole change; nothing outside this file needs to know.
//! `crate::util::lzf` (F0) already provides the codec.

use std::collections::VecDeque;

use smallvec::SmallVec;

use super::listpack::{Listpack, ListpackEntry, ListpackIter};

/// `quicklist.c: optimization_level`, indexed by `-fill - 1`.
const OPTIMIZATION_LEVEL: [usize; 5] = [4096, 8192, 16384, 32768, 65536];
/// `quicklist.c: SIZE_SAFETY_LIMIT`.
const SIZE_SAFETY_LIMIT: usize = 8192;
/// `quicklist.c: SIZE_ESTIMATE_OVERHEAD` -- per-entry listpack overhead used
/// when deciding whether one more entry still fits.
const SIZE_ESTIMATE_OVERHEAD: usize = 11;

/// Redis's default `list-max-listpack-size`.
pub const DEFAULT_FILL: i64 = 128;

/// An owned popped value. Inline up to 32 bytes, so `LPOP` of a typical
/// element never touches the allocator.
pub type QlValue = SmallVec<[u8; 32]>;

/// `quicklist.c: quicklistNodeLimit` -- `(size_limit, count_limit)`, where 0
/// means "not this kind of limit".
#[inline]
fn node_limit(fill: i64) -> (usize, usize) {
    if fill >= 0 {
        // Ensure a node can always hold at least one entry.
        (0, if fill == 0 { 1 } else { fill as usize })
    } else {
        let idx = ((-fill) as usize).clamp(1, OPTIMIZATION_LEVEL.len()) - 1;
        (OPTIMIZATION_LEVEL.get(idx).copied().unwrap_or(8192), 0)
    }
}

/// `quicklist.c: quicklistNodeExceedsLimit`.
#[inline]
fn exceeds_limit(fill: i64, new_sz: usize, new_count: usize) -> bool {
    let (sz_limit, count_limit) = node_limit(fill);
    if sz_limit != 0 {
        return new_sz > sz_limit;
    }
    if count_limit != 0 {
        // A count limit is only safe while the node stays under the hard size
        // ceiling; Redis checks both.
        if new_sz > SIZE_SAFETY_LIMIT {
            return true;
        }
        return new_count > count_limit;
    }
    false
}

/// `quicklist.c: isLargeElement` -- too big to share a listpack with anything.
#[inline]
fn is_large_element(sz: usize, fill: i64) -> bool {
    if fill >= 0 {
        sz > SIZE_SAFETY_LIMIT
    } else {
        let (limit, _) = node_limit(fill);
        sz > limit
    }
}

/// What a node holds.
#[derive(Debug, Clone)]
enum NodeData {
    /// The normal case: many entries in one listpack.
    Packed(Listpack),
    /// A single element too large to pack (`QL_NODE_IS_PLAIN`).
    Plain(Box<[u8]>),
    // Seam for LZF: `Lzf(Box<[u8]>, usize /* decompressed len */)`.
}

#[derive(Debug, Clone)]
struct Node {
    data: NodeData,
}

impl Node {
    #[inline]
    fn count(&self) -> usize {
        match &self.data {
            NodeData::Packed(lp) => lp.len(),
            NodeData::Plain(_) => 1,
        }
    }

    #[inline]
    fn bytes(&self) -> usize {
        match &self.data {
            NodeData::Packed(lp) => lp.total_bytes(),
            NodeData::Plain(b) => b.len(),
        }
    }

    #[inline]
    fn is_plain(&self) -> bool {
        matches!(self.data, NodeData::Plain(_))
    }

    /// The node's listpack, or `None` for a plain node. This is the seam an
    /// LZF variant would decompress behind.
    #[inline]
    #[cfg(test)]
    fn lp(&self) -> Option<&Listpack> {
        match &self.data {
            NodeData::Packed(lp) => Some(lp),
            NodeData::Plain(_) => None,
        }
    }

    #[inline]
    fn lp_mut(&mut self) -> Option<&mut Listpack> {
        match &mut self.data {
            NodeData::Packed(lp) => Some(lp),
            NodeData::Plain(_) => None,
        }
    }

    #[inline]
    fn get(&self, i: usize) -> Option<ListpackEntry<'_>> {
        match &self.data {
            NodeData::Packed(lp) => lp.get(i as isize),
            NodeData::Plain(b) => {
                if i == 0 {
                    Some(ListpackEntry::Str(b))
                } else {
                    None
                }
            }
        }
    }

    /// Can one more `sz`-byte element go in here? (`_quicklistNodeAllowInsert`)
    #[inline]
    fn allows_insert(&self, fill: i64, sz: usize) -> bool {
        if self.is_plain() || is_large_element(sz, fill) {
            return false;
        }
        let new_sz = self.bytes() + sz + SIZE_ESTIMATE_OVERHEAD;
        !exceeds_limit(fill, new_sz, self.count() + 1)
    }

    /// Can these two nodes be one node? (`_quicklistNodeAllowMerge`)
    #[inline]
    fn allows_merge(a: &Node, b: &Node, fill: i64) -> bool {
        if a.is_plain() || b.is_plain() {
            return false;
        }
        // The two listpack headers overlap once merged, hence the -11.
        let merge_sz = a.bytes() + b.bytes() - 11;
        !exceeds_limit(fill, merge_sz, a.count() + b.count())
    }
}

/// A list of listpack nodes.
#[derive(Debug, Clone)]
pub struct Quicklist {
    nodes: VecDeque<Node>,
    count: usize,
    fill: i64,
    compress_depth: u32,
}

impl Default for Quicklist {
    #[inline]
    fn default() -> Self {
        Self::new(DEFAULT_FILL)
    }
}

impl Quicklist {
    /// A new list with the given `list-max-listpack-size`.
    #[inline]
    pub fn new(fill: i64) -> Self {
        Quicklist {
            nodes: VecDeque::new(),
            count: 0,
            fill,
            compress_depth: 0,
        }
    }

    /// Build from an iterator of values.
    pub fn from_values<I, B>(fill: i64, items: I) -> Self
    where
        I: IntoIterator<Item = B>,
        B: AsRef<[u8]>,
    {
        let mut ql = Quicklist::new(fill);
        for v in items {
            ql.push_tail(v.as_ref());
        }
        ql
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.count
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Number of listpack nodes. Exposed for `DEBUG QUICKLIST-PACKED-THRESHOLD`
    /// style introspection and for tests that assert on merge/split.
    #[inline]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[inline]
    pub fn fill(&self) -> i64 {
        self.fill
    }

    /// Change `list-max-listpack-size`. Existing nodes are left as they are,
    /// exactly as Redis does on `CONFIG SET`; only new fills obey the value.
    #[inline]
    pub fn set_fill(&mut self, fill: i64) {
        self.fill = fill;
    }

    /// `list-compress-depth`. Stored and reported but **not yet acted on** --
    /// see the module header.
    #[inline]
    pub fn compress_depth(&self) -> u32 {
        self.compress_depth
    }

    #[inline]
    pub fn set_compress_depth(&mut self, d: u32) {
        self.compress_depth = d;
    }

    /// Approximate heap footprint, for `MEMORY USAGE`.
    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>()
            + self.nodes.capacity() * core::mem::size_of::<Node>()
            + self
                .nodes
                .iter()
                .map(|n| match &n.data {
                    NodeData::Packed(lp) => lp.mem_usage(),
                    NodeData::Plain(b) => b.len(),
                })
                .sum::<usize>()
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.count = 0;
    }

    #[inline]
    fn new_node(&self, v: &[u8]) -> Node {
        if is_large_element(v.len(), self.fill) {
            Node {
                data: NodeData::Plain(v.to_vec().into_boxed_slice()),
            }
        } else {
            let mut lp = Listpack::with_capacity(v.len() + 11);
            lp.append(ListpackEntry::Str(v));
            Node {
                data: NodeData::Packed(lp),
            }
        }
    }

    // ---- push / pop -----------------------------------------------------

    /// `quicklistPushHead`. Returns true when a new node was created.
    pub fn push_head(&mut self, v: &[u8]) -> bool {
        self.count += 1;
        if let Some(n) = self.nodes.front_mut()
            && n.allows_insert(self.fill, v.len())
            && let Some(lp) = n.lp_mut()
            && lp.prepend(ListpackEntry::Str(v))
        {
            return false;
        }
        let node = self.new_node(v);
        self.nodes.push_front(node);
        true
    }

    /// `quicklistPushTail`. Returns true when a new node was created.
    pub fn push_tail(&mut self, v: &[u8]) -> bool {
        self.count += 1;
        if let Some(n) = self.nodes.back_mut()
            && n.allows_insert(self.fill, v.len())
            && let Some(lp) = n.lp_mut()
            && lp.append(ListpackEntry::Str(v))
        {
            return false;
        }
        let node = self.new_node(v);
        self.nodes.push_back(node);
        true
    }

    /// The first element, borrowed. No allocation (§5.1).
    #[inline]
    pub fn head(&self) -> Option<ListpackEntry<'_>> {
        self.nodes.front()?.get(0)
    }

    /// The last element, borrowed.
    #[inline]
    pub fn tail(&self) -> Option<ListpackEntry<'_>> {
        let n = self.nodes.back()?;
        match &n.data {
            NodeData::Packed(lp) => lp.last(),
            NodeData::Plain(b) => Some(ListpackEntry::Str(b)),
        }
    }

    /// Drop the first element without materialising it. Pairs with
    /// [`Self::head`] when the caller has already written the value out.
    pub fn drop_head(&mut self) -> bool {
        let Some(n) = self.nodes.front_mut() else {
            return false;
        };
        match &mut n.data {
            NodeData::Packed(lp) => {
                if !lp.delete(0) {
                    return false;
                }
                if lp.is_empty() {
                    self.nodes.pop_front();
                }
            }
            NodeData::Plain(_) => {
                self.nodes.pop_front();
            }
        }
        self.count -= 1;
        true
    }

    /// Drop the last element without materialising it.
    pub fn drop_tail(&mut self) -> bool {
        let Some(n) = self.nodes.back_mut() else {
            return false;
        };
        match &mut n.data {
            NodeData::Packed(lp) => {
                if !lp.delete(-1) {
                    return false;
                }
                if lp.is_empty() {
                    self.nodes.pop_back();
                }
            }
            NodeData::Plain(_) => {
                self.nodes.pop_back();
            }
        }
        self.count -= 1;
        true
    }

    /// `LPOP`. Inline for values up to 32 bytes.
    pub fn pop_head(&mut self) -> Option<QlValue> {
        let v = self.head()?.to_buf();
        self.drop_head();
        Some(v)
    }

    /// `RPOP`.
    pub fn pop_tail(&mut self) -> Option<QlValue> {
        let v = self.tail()?.to_buf();
        self.drop_tail();
        Some(v)
    }

    // ---- indexing --------------------------------------------------------

    /// Normalise a possibly negative index into `0..count`.
    #[inline]
    fn normalize(&self, index: i64) -> Option<usize> {
        let i = if index < 0 {
            index.checked_add(self.count as i64)?
        } else {
            index
        };
        if i < 0 || i as usize >= self.count {
            None
        } else {
            Some(i as usize)
        }
    }

    /// Resolve a normalised index to `(node, offset within node)`, walking
    /// from whichever end is closer.
    fn locate(&self, i: usize) -> Option<(usize, usize)> {
        if i >= self.count {
            return None;
        }
        if i <= self.count / 2 {
            let mut acc = 0usize;
            for (ni, n) in self.nodes.iter().enumerate() {
                let c = n.count();
                if i < acc + c {
                    return Some((ni, i - acc));
                }
                acc += c;
            }
        } else {
            let mut acc = self.count;
            for (ni, n) in self.nodes.iter().enumerate().rev() {
                let c = n.count();
                acc -= c;
                if i >= acc {
                    return Some((ni, i - acc));
                }
            }
        }
        None
    }

    /// `LINDEX`, negative indices included. Borrowed, no allocation.
    #[inline]
    pub fn index(&self, index: i64) -> Option<ListpackEntry<'_>> {
        let i = self.normalize(index)?;
        let (ni, off) = self.locate(i)?;
        self.nodes.get(ni)?.get(off)
    }

    /// `LSET`.
    pub fn set(&mut self, index: i64, v: &[u8]) -> bool {
        let Some(i) = self.normalize(index) else {
            return false;
        };
        let Some((ni, off)) = self.locate(i) else {
            return false;
        };
        let fill = self.fill;
        let large = is_large_element(v.len(), fill);
        let Some(n) = self.nodes.get_mut(ni) else {
            return false;
        };
        match (&mut n.data, large) {
            (NodeData::Plain(b), true) => {
                *b = v.to_vec().into_boxed_slice();
                true
            }
            (NodeData::Packed(lp), false) => lp.replace(off, ListpackEntry::Str(v)),
            // Crossing the plain/packed boundary: rebuild via delete + insert.
            _ => {
                self.delete_range(i as i64, 1);
                self.insert_at(i, v)
            }
        }
    }

    // ---- insert ----------------------------------------------------------

    /// `LINSERT ... BEFORE`: insert `v` so it lands at position `index`.
    #[inline]
    pub fn insert_before(&mut self, index: i64, v: &[u8]) -> bool {
        let Some(i) = self.normalize(index) else {
            return false;
        };
        self.insert_at(i, v)
    }

    /// `LINSERT ... AFTER`: insert `v` just past position `index`.
    #[inline]
    pub fn insert_after(&mut self, index: i64, v: &[u8]) -> bool {
        let Some(i) = self.normalize(index) else {
            return false;
        };
        self.insert_at(i + 1, v)
    }

    /// Insert so that `v` ends up at position `i`. `i == len()` appends.
    fn insert_at(&mut self, i: usize, v: &[u8]) -> bool {
        if i == 0 {
            self.push_head(v);
            return true;
        }
        if i >= self.count {
            self.push_tail(v);
            return true;
        }
        let Some((ni, off)) = self.locate(i) else {
            return false;
        };
        let fill = self.fill;

        // 1. Straight into the target node.
        if let Some(n) = self.nodes.get_mut(ni)
            && n.allows_insert(fill, v.len())
            && let Some(lp) = n.lp_mut()
            && lp.insert(off, ListpackEntry::Str(v))
        {
            self.count += 1;
            return true;
        }

        // 2. At a node boundary, try the neighbour rather than splitting.
        if off == 0
            && ni > 0
            && let Some(prev) = self.nodes.get_mut(ni - 1)
            && prev.allows_insert(fill, v.len())
            && let Some(lp) = prev.lp_mut()
            && lp.append(ListpackEntry::Str(v))
        {
            self.count += 1;
            return true;
        }

        // 3. On a node boundary, or in front of a plain node, there is
        //    nothing to split: the element becomes its own node. Splitting at
        //    `off == 0` would leave an empty node behind, and an empty node
        //    breaks `head()` / `pop_head()` -- Redis never keeps one either.
        let Some(n) = self.nodes.get_mut(ni) else {
            return false;
        };
        if off == 0 || n.is_plain() {
            let node = self.new_node(v);
            self.nodes.insert(ni, node);
            self.count += 1;
            self.merge_around(ni);
            return true;
        }

        // 4. Split the node at `off` and put the new element between the
        //    halves, as its own node. The merge pass folds the pieces back
        //    together when they fit.
        let Some(lp) = n.lp_mut() else { return false };
        let total = lp.len();
        let tail: Listpack = lp.iter().skip(off).collect();
        let removed = total - off;
        if let Some(from) = lp.seek(off as isize) {
            let to = lp.eof_offset();
            lp.delete_byte_range(from, to, removed);
        }
        let mid = self.new_node(v);
        self.nodes.insert(
            ni + 1,
            Node {
                data: NodeData::Packed(tail),
            },
        );
        self.nodes.insert(ni + 1, mid);
        self.count += 1;
        self.merge_around(ni + 1);
        true
    }

    /// Try to fold node `ni` into its neighbours (`_quicklistMergeNodes`).
    fn merge_around(&mut self, ni: usize) {
        // Merge with the previous node first, then with the next.
        let mut at = ni;
        if at > 0 && self.try_merge(at - 1) {
            at -= 1;
        }
        self.try_merge(at);
    }

    /// Merge node `i` with node `i + 1` when the result still fits.
    fn try_merge(&mut self, i: usize) -> bool {
        let (Some(a), Some(b)) = (self.nodes.get(i), self.nodes.get(i + 1)) else {
            return false;
        };
        if !Node::allows_merge(a, b, self.fill) {
            return false;
        }
        let Some(next) = self.nodes.remove(i + 1) else {
            return false;
        };
        let NodeData::Packed(src) = next.data else {
            return false;
        };
        if let Some(dst) = self.nodes.get_mut(i).and_then(Node::lp_mut) {
            dst.append_all(&src);
        }
        true
    }

    // ---- delete ----------------------------------------------------------

    /// `quicklistDelRange`: remove up to `count` elements starting at `start`
    /// (negative counts from the end). Returns how many went.
    pub fn delete_range(&mut self, start: i64, count: usize) -> usize {
        if count == 0 || self.count == 0 {
            return 0;
        }
        let Some(i) = self.normalize(start) else {
            return 0;
        };
        let want = count.min(self.count - i);
        let mut left = want;

        while left > 0 {
            let Some((ni, off)) = self.locate(i) else {
                break;
            };
            let Some(n) = self.nodes.get_mut(ni) else {
                break;
            };
            match &mut n.data {
                NodeData::Plain(_) => {
                    self.nodes.remove(ni);
                    self.count -= 1;
                    left -= 1;
                }
                NodeData::Packed(lp) => {
                    let here = (lp.len() - off).min(left);
                    let got = lp.delete_range(off, here);
                    if got == 0 {
                        break;
                    }
                    self.count -= got;
                    left -= got;
                    if lp.is_empty() {
                        self.nodes.remove(ni);
                    }
                }
            }
            // `i` stays put: everything after the hole shifted down into it.
        }
        let removed = want - left;
        // A deletion can leave two small neighbours where one node would do.
        if removed > 0
            && !self.nodes.is_empty()
            && let Some((ni, _)) = self.locate(i.min(self.count - 1))
        {
            self.merge_around(ni);
        }
        removed
    }

    /// Remove occurrences of `v`: the first `count` scanning forward when
    /// `count > 0`, the last `|count|` scanning backward when `count < 0`, or
    /// every one when `count == 0`. That is `LREM`'s contract exactly.
    /// Returns how many were removed.
    pub fn remove_value(&mut self, v: &[u8], count: i64) -> usize {
        let limit = if count == 0 {
            usize::MAX
        } else {
            count.unsigned_abs() as usize
        };
        let backward = count < 0;
        let mut removed = 0usize;
        while removed < limit {
            let found = if backward {
                self.rposition(v)
            } else {
                self.position(v)
            };
            let Some(idx) = found else { break };
            if self.delete_range(idx as i64, 1) == 0 {
                break;
            }
            removed += 1;
        }
        removed
    }

    /// Index of the first element equal to `v`.
    pub fn position(&self, v: &[u8]) -> Option<usize> {
        self.iter().position(|e| e.eq_bytes(v))
    }

    /// Index of the last element equal to `v`.
    pub fn rposition(&self, v: &[u8]) -> Option<usize> {
        let n = self.count;
        self.iter_rev()
            .position(|e| e.eq_bytes(v))
            .map(|k| n - 1 - k)
    }

    // ---- iteration -------------------------------------------------------

    /// Forward iterator over every element.
    #[inline]
    pub fn iter(&self) -> QuicklistIter<'_> {
        QuicklistIter {
            ql: self,
            node: 0,
            cur: None,
            remaining: self.count,
        }
    }

    /// Forward iterator starting at element `index` (negative allowed).
    pub fn iter_from(&self, index: i64) -> QuicklistIter<'_> {
        let Some(i) = self.normalize(index) else {
            return QuicklistIter {
                ql: self,
                node: self.nodes.len(),
                cur: None,
                remaining: 0,
            };
        };
        let mut it = QuicklistIter {
            ql: self,
            node: 0,
            cur: None,
            remaining: self.count - i,
        };
        if let Some((ni, off)) = self.locate(i) {
            // `node` is the *next* node to open, so it points past `ni`.
            it.node = ni + 1;
            it.cur = self.nodes.get(ni).map(|n| node_cursor(n, off));
        }
        it
    }

    /// Reverse iterator over every element.
    #[inline]
    pub fn iter_rev(&self) -> QuicklistRevIter<'_> {
        QuicklistRevIter {
            ql: self,
            node: self.nodes.len(),
            buf: SmallVec::new(),
            pos: 0,
            remaining: self.count,
        }
    }
}

/// Position inside one node.
#[derive(Clone)]
enum Cursor<'a> {
    Packed(ListpackIter<'a>),
    Plain(Option<&'a [u8]>),
}

fn node_cursor(n: &Node, off: usize) -> Cursor<'_> {
    match &n.data {
        NodeData::Packed(lp) => {
            let start = lp.seek(off as isize).unwrap_or(lp.eof_offset());
            Cursor::Packed(lp.iter_from(start))
        }
        NodeData::Plain(b) => Cursor::Plain(if off == 0 { Some(b) } else { None }),
    }
}

impl<'a> Cursor<'a> {
    #[inline]
    fn next(&mut self) -> Option<ListpackEntry<'a>> {
        match self {
            Cursor::Packed(it) => it.next(),
            Cursor::Plain(v) => v.take().map(ListpackEntry::Str),
        }
    }
}

/// Forward iterator over quicklist elements.
#[derive(Clone)]
pub struct QuicklistIter<'a> {
    ql: &'a Quicklist,
    node: usize,
    cur: Option<Cursor<'a>>,
    remaining: usize,
}

impl<'a> Iterator for QuicklistIter<'a> {
    type Item = ListpackEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        loop {
            if let Some(c) = self.cur.as_mut()
                && let Some(e) = c.next()
            {
                self.remaining -= 1;
                return Some(e);
            }
            // Current node exhausted: open the next one. `?` ends iteration
            // when there is none, and an empty node simply loops again.
            let n = self.ql.nodes.get(self.node)?;
            self.cur = Some(node_cursor(n, 0));
            self.node += 1;
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// Reverse iterator over quicklist elements.
///
/// Listpack reverse traversal is offset-based rather than iterator-based, so
/// this walks a node backwards by collecting its entry offsets once. Offsets,
/// not values -- the yielded entries still borrow the listpack directly.
pub struct QuicklistRevIter<'a> {
    ql: &'a Quicklist,
    node: usize,
    buf: SmallVec<[usize; 32]>,
    pos: usize,
    remaining: usize,
}

impl<'a> Iterator for QuicklistRevIter<'a> {
    type Item = ListpackEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        loop {
            if self.pos > 0 {
                self.pos -= 1;
                let off = *self.buf.get(self.pos)?;
                let n = self.ql.nodes.get(self.node)?;
                self.remaining -= 1;
                return match &n.data {
                    NodeData::Packed(lp) => lp.entry_at(off),
                    NodeData::Plain(b) => Some(ListpackEntry::Str(b)),
                };
            }
            if self.node == 0 {
                return None;
            }
            self.node -= 1;
            let n = self.ql.nodes.get(self.node)?;
            self.buf.clear();
            match &n.data {
                NodeData::Packed(lp) => {
                    let mut off = lp.first_offset();
                    while off < lp.eof_offset() {
                        self.buf.push(off);
                        match lp.next_offset(off) {
                            Some(o) => off = o,
                            None => break,
                        }
                    }
                }
                NodeData::Plain(_) => self.buf.push(0),
            }
            self.pos = self.buf.len();
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vals(ql: &Quicklist) -> Vec<Vec<u8>> {
        ql.iter().map(|e| e.to_buf().to_vec()).collect()
    }

    fn strs(ql: &Quicklist) -> Vec<String> {
        ql.iter()
            .map(|e| String::from_utf8_lossy(&e.to_buf()).into_owned())
            .collect()
    }

    #[test]
    fn empty() {
        let mut ql = Quicklist::new(DEFAULT_FILL);
        assert!(ql.is_empty());
        assert_eq!(ql.len(), 0);
        assert_eq!(ql.node_count(), 0);
        assert!(ql.head().is_none() && ql.tail().is_none());
        assert!(ql.pop_head().is_none() && ql.pop_tail().is_none());
        assert!(ql.index(0).is_none() && ql.index(-1).is_none());
        assert_eq!(ql.iter().count(), 0);
        assert_eq!(ql.iter_rev().count(), 0);
        assert_eq!(ql.delete_range(0, 5), 0);
    }

    #[test]
    fn push_and_pop_both_ends() {
        let mut ql = Quicklist::new(DEFAULT_FILL);
        ql.push_tail(b"b");
        ql.push_tail(b"c");
        ql.push_head(b"a");
        assert_eq!(strs(&ql), vec!["a", "b", "c"]);
        assert_eq!(ql.len(), 3);
        assert_eq!(ql.pop_head().as_deref(), Some(&b"a"[..]));
        assert_eq!(ql.pop_tail().as_deref(), Some(&b"c"[..]));
        assert_eq!(strs(&ql), vec!["b"]);
        assert_eq!(ql.pop_head().as_deref(), Some(&b"b"[..]));
        assert!(ql.is_empty());
        assert_eq!(ql.node_count(), 0, "empty nodes must be reaped");
    }

    #[test]
    fn count_fill_creates_nodes_at_the_boundary() {
        let mut ql = Quicklist::new(4);
        for i in 0..9 {
            ql.push_tail(format!("v{i}").as_bytes());
        }
        assert_eq!(ql.len(), 9);
        assert_eq!(ql.node_count(), 3, "4 + 4 + 1");
        assert_eq!(
            strs(&ql),
            (0..9).map(|i| format!("v{i}")).collect::<Vec<_>>()
        );
    }

    #[test]
    fn size_fill_creates_nodes_at_the_boundary() {
        // fill = -1 -> 4 KB nodes.
        let mut ql = Quicklist::new(-1);
        let chunk = vec![b'x'; 500];
        for _ in 0..20 {
            ql.push_tail(&chunk);
        }
        assert_eq!(ql.len(), 20);
        assert!(ql.node_count() >= 3, "got {}", ql.node_count());
        for n in &ql.nodes {
            assert!(n.bytes() <= 4096, "node overflowed the size class");
        }
    }

    #[test]
    fn large_elements_become_plain_nodes() {
        let mut ql = Quicklist::new(-2); // 8 KB
        ql.push_tail(b"small");
        let big = vec![b'z'; 20_000];
        ql.push_tail(&big);
        ql.push_tail(b"after");
        assert_eq!(ql.len(), 3);
        assert_eq!(ql.node_count(), 3);
        assert!(ql.nodes[1].is_plain());
        assert_eq!(
            ql.index(1).and_then(|e| e.as_bytes()).map(<[u8]>::len),
            Some(20_000)
        );
        assert_eq!(ql.index(2).and_then(|e| e.as_bytes()), Some(&b"after"[..]));
        // And it round-trips through iteration in both directions.
        assert_eq!(ql.iter().count(), 3);
        assert_eq!(ql.iter_rev().count(), 3);
        assert_eq!(ql.pop_tail().as_deref(), Some(&b"after"[..]));
        assert_eq!(ql.pop_tail().map(|v| v.len()), Some(20_000));
        assert_eq!(ql.len(), 1);
    }

    #[test]
    fn index_negative_and_out_of_range() {
        let ql = Quicklist::from_values(3, (0..10).map(|i| format!("v{i}")));
        for i in 0..10i64 {
            assert_eq!(
                ql.index(i).map(|e| e.to_buf().to_vec()),
                Some(format!("v{i}").into_bytes())
            );
            assert_eq!(
                ql.index(i - 10).map(|e| e.to_buf().to_vec()),
                Some(format!("v{i}").into_bytes())
            );
        }
        assert!(ql.index(10).is_none());
        assert!(ql.index(-11).is_none());
    }

    #[test]
    fn set_in_place_and_across_the_plain_boundary() {
        let mut ql = Quicklist::from_values(4, ["a", "b", "c", "d", "e"]);
        assert!(ql.set(1, b"BB"));
        assert_eq!(strs(&ql), vec!["a", "BB", "c", "d", "e"]);
        assert!(ql.set(-1, b"EE"));
        assert_eq!(strs(&ql), vec!["a", "BB", "c", "d", "EE"]);
        assert!(!ql.set(99, b"x"));

        // Packed -> plain.
        let big = vec![b'q'; 20_000];
        assert!(ql.set(2, &big));
        assert_eq!(ql.len(), 5);
        assert_eq!(
            ql.index(2).and_then(|e| e.as_bytes()).map(<[u8]>::len),
            Some(20_000)
        );
        // Plain -> packed.
        assert!(ql.set(2, b"cc"));
        assert_eq!(strs(&ql), vec!["a", "BB", "cc", "d", "EE"]);
    }

    #[test]
    fn insert_before_and_after_split_nodes() {
        let mut ql = Quicklist::new(2);
        for v in ["a", "b", "c", "d"] {
            ql.push_tail(v.as_bytes());
        }
        assert_eq!(ql.node_count(), 2);
        assert!(ql.insert_before(2, b"X")); // between b and c
        assert_eq!(strs(&ql), vec!["a", "b", "X", "c", "d"]);
        assert_eq!(ql.len(), 5);
        assert!(ql.insert_after(0, b"Y"));
        assert_eq!(strs(&ql), vec!["a", "Y", "b", "X", "c", "d"]);
        assert!(ql.insert_before(0, b"HEAD"));
        assert_eq!(
            ql.head().map(|e| e.to_buf().to_vec()),
            Some(b"HEAD".to_vec())
        );
        assert!(ql.insert_after(-1, b"TAIL"));
        assert_eq!(
            ql.tail().map(|e| e.to_buf().to_vec()),
            Some(b"TAIL".to_vec())
        );
        assert_eq!(ql.len(), 8);
        assert!(!ql.insert_before(99, b"nope"));
        // Everything still reads back in order and in reverse.
        let mut rev: Vec<_> = ql.iter_rev().map(|e| e.to_buf().to_vec()).collect();
        rev.reverse();
        assert_eq!(rev, vals(&ql));
    }

    /// Regression, found by `prop_quicklist_matches_vec_model_size_fill`.
    ///
    /// Inserting at offset 0 of a node used to go down the split path, which
    /// left an empty listpack behind. Usually the merge pass swallowed it, so
    /// the bug hid -- but when the inserted element is large enough to need a
    /// plain node, neither neighbour can merge with the empty node and it
    /// survives. Once such a node reaches the front, `head()` and `pop_head()`
    /// read it as "no entries" and report a non-empty list as empty.
    #[test]
    fn inserting_a_large_element_at_a_node_boundary_leaves_no_empty_node() {
        let big = vec![b'B'; 9000];
        let mut ql = Quicklist::new(-1); // 4 KB nodes
        for v in ["c", "b", "a"] {
            ql.push_head(v.as_bytes());
        }
        ql.push_head(&big); // too large to pack: its own plain node, at the front
        assert_eq!(ql.node_count(), 2);

        // Index 1 is the first entry of the packed node. That node cannot take
        // a 9000-byte element, and neither can the plain node in front of it,
        // so this is exactly the case that used to split at offset 0.
        assert!(ql.insert_before(1, &big));
        assert_eq!(ql.len(), 5);

        // Every node must hold at least one entry.
        assert_eq!(ql.iter().count(), 5);
        assert_eq!(ql.iter_rev().count(), 5);
        assert_eq!(
            ql.head().and_then(|e| e.as_bytes()).map(<[u8]>::len),
            Some(9000)
        );
        assert_eq!(ql.tail().map(|e| e.to_buf().to_vec()), Some(b"c".to_vec()));

        // Draining from the head must yield all five, in order.
        let mut drained = Vec::new();
        while let Some(v) = ql.pop_head() {
            drained.push(v.to_vec());
        }
        assert_eq!(drained.len(), 5);
        assert_eq!(drained[0].len(), 9000);
        assert_eq!(drained[1].len(), 9000);
        assert_eq!(
            &drained[2..],
            &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
        assert!(ql.is_empty());
        assert_eq!(ql.node_count(), 0);
    }

    #[test]
    fn insert_in_the_middle_of_a_full_node_splits_then_merges() {
        let mut ql = Quicklist::new(100);
        for i in 0..100 {
            ql.push_tail(format!("v{i:03}").as_bytes());
        }
        assert_eq!(ql.node_count(), 1);
        assert!(ql.insert_before(50, b"MID"));
        assert_eq!(ql.len(), 101);
        assert_eq!(
            ql.index(50).map(|e| e.to_buf().to_vec()),
            Some(b"MID".to_vec())
        );
        assert_eq!(
            ql.index(49).map(|e| e.to_buf().to_vec()),
            Some(b"v049".to_vec())
        );
        assert_eq!(
            ql.index(51).map(|e| e.to_buf().to_vec()),
            Some(b"v050".to_vec())
        );
        // Every element is still exactly once in the list.
        assert_eq!(vals(&ql).len(), 101);
    }

    #[test]
    fn delete_range_spans_nodes() {
        let mut ql = Quicklist::from_values(3, (0..12).map(|i| format!("v{i:02}")));
        assert_eq!(ql.node_count(), 4);
        assert_eq!(ql.delete_range(2, 7), 7);
        assert_eq!(strs(&ql), vec!["v00", "v01", "v09", "v10", "v11"]);
        assert_eq!(ql.len(), 5);
        assert_eq!(ql.delete_range(-2, 5), 2, "count clamps to the end");
        assert_eq!(strs(&ql), vec!["v00", "v01", "v09"]);
        assert_eq!(ql.delete_range(0, 100), 3);
        assert!(ql.is_empty());
        assert_eq!(ql.node_count(), 0);
        assert_eq!(ql.delete_range(0, 1), 0);
    }

    #[test]
    fn delete_the_only_element() {
        let mut ql = Quicklist::new(DEFAULT_FILL);
        ql.push_tail(b"solo");
        assert_eq!(ql.delete_range(0, 1), 1);
        assert!(ql.is_empty());
        assert_eq!(ql.node_count(), 0);
    }

    #[test]
    fn reverse_iteration_after_deletion() {
        let mut ql = Quicklist::from_values(4, (0..20).map(|i| format!("v{i:02}")));
        ql.delete_range(0, 3);
        ql.delete_range(-2, 2);
        let fwd = vals(&ql);
        let mut rev: Vec<_> = ql.iter_rev().map(|e| e.to_buf().to_vec()).collect();
        rev.reverse();
        assert_eq!(fwd, rev);
        assert_eq!(fwd.len(), 15);
        assert_eq!(fwd.first().map(|v| v.as_slice()), Some(&b"v03"[..]));
        assert_eq!(fwd.last().map(|v| v.as_slice()), Some(&b"v17"[..]));
    }

    #[test]
    fn iter_from_offset() {
        let ql = Quicklist::from_values(3, (0..10).map(|i| format!("v{i}")));
        assert_eq!(
            ql.iter_from(7)
                .map(|e| e.to_buf().to_vec())
                .collect::<Vec<_>>(),
            vec![b"v7".to_vec(), b"v8".to_vec(), b"v9".to_vec()]
        );
        assert_eq!(ql.iter_from(-1).count(), 1);
        assert_eq!(ql.iter_from(0).count(), 10);
        assert_eq!(ql.iter_from(10).count(), 0);
    }

    #[test]
    fn lrem_semantics() {
        let mut ql = Quicklist::from_values(3, ["a", "b", "a", "c", "a", "b"]);
        assert_eq!(ql.position(b"a"), Some(0));
        assert_eq!(ql.rposition(b"a"), Some(4));
        assert_eq!(ql.remove_value(b"a", 2), 2);
        assert_eq!(strs(&ql), vec!["b", "c", "a", "b"]);

        let mut ql = Quicklist::from_values(3, ["a", "b", "a", "c", "a", "b"]);
        assert_eq!(ql.remove_value(b"a", -2), 2);
        assert_eq!(strs(&ql), vec!["a", "b", "c", "b"]);

        let mut ql = Quicklist::from_values(3, ["a", "b", "a", "c", "a", "b"]);
        assert_eq!(ql.remove_value(b"a", 0), 3);
        assert_eq!(strs(&ql), vec!["b", "c", "b"]);
        assert_eq!(ql.remove_value(b"zz", 0), 0);
    }

    #[test]
    fn numeric_values_round_trip_as_text() {
        // Listpack int-encodes "123"; LRANGE must still print "123".
        let ql = Quicklist::from_values(DEFAULT_FILL, ["123", "0123", "-7"]);
        assert_eq!(strs(&ql), vec!["123", "0123", "-7"]);
        assert_eq!(ql.position(b"123"), Some(0));
        assert_eq!(ql.position(b"0123"), Some(1));
    }

    #[test]
    fn node_limit_matches_redis_table() {
        assert_eq!(node_limit(0), (0, 1));
        assert_eq!(node_limit(1), (0, 1));
        assert_eq!(node_limit(128), (0, 128));
        assert_eq!(node_limit(-1), (4096, 0));
        assert_eq!(node_limit(-2), (8192, 0));
        assert_eq!(node_limit(-3), (16384, 0));
        assert_eq!(node_limit(-4), (32768, 0));
        assert_eq!(node_limit(-5), (65536, 0));
        // Out-of-range negatives clamp instead of panicking.
        assert_eq!(node_limit(-99), (65536, 0));
    }

    #[test]
    fn count_fill_still_respects_the_hard_size_ceiling() {
        // fill = 128 by count, but 128 * 200 bytes blows past SIZE_SAFETY_LIMIT,
        // so Redis splits early. We must too.
        let mut ql = Quicklist::new(128);
        let chunk = vec![b'x'; 200];
        for _ in 0..128 {
            ql.push_tail(&chunk);
        }
        assert!(ql.node_count() > 1, "size safety limit was ignored");
        for n in &ql.nodes {
            assert!(n.bytes() <= SIZE_SAFETY_LIMIT + 300);
        }
        assert_eq!(ql.len(), 128);
    }

    #[test]
    fn compress_depth_is_stored_but_inert() {
        let mut ql = Quicklist::new(DEFAULT_FILL);
        assert_eq!(ql.compress_depth(), 0);
        ql.set_compress_depth(2);
        assert_eq!(ql.compress_depth(), 2);
        for i in 0..1000 {
            ql.push_tail(format!("v{i}").as_bytes());
        }
        // No compression yet: every node is still a live listpack.
        assert!(ql.nodes.iter().all(|n| n.lp().is_some()));
        assert_eq!(ql.len(), 1000);
    }
}
