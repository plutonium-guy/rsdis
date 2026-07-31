//! The string object.
//!
//! Owned by W2a; do not edit if you are not that agent.
//!
//! F0 seeded this with a *working* minimal implementation (not an empty stub)
//! because `SET`/`GET` have to run end to end for the foundation to be
//! provably alive. W2a extends it -- bit operations, `APPEND`/`SETRANGE`
//! in-place growth, `GETRANGE`, and the sparse/dense HyperLogLog layouts that
//! ride on top of it -- but must keep:
//!
//!   * the three `OBJECT ENCODING` values `int` / `embstr` / `raw`, with the
//!     44-byte embstr boundary and the int-encoding acceptance rules;
//!   * `as_slice()` returning `None` for int-encoded values, so that the reply
//!     path can format the integer straight into the output buffer instead of
//!     materialising a string (§5.1).

use bytes::Bytes;

use crate::util::strnum;

/// Longest string Redis will store with the `embstr` encoding.
pub const EMBSTR_LIMIT: usize = 44;
/// Longest decimal string Redis will consider for the `int` encoding.
const INT_ENCODING_MAX_LEN: usize = 20;

#[derive(Debug, Clone)]
pub enum StrObj {
    /// `OBJECT ENCODING` -> `int`
    Int(i64),
    /// `OBJECT ENCODING` -> `embstr` (len <= 44) or `raw`
    Str { data: Bytes, embstr: bool },
}

impl Default for StrObj {
    fn default() -> Self {
        StrObj::Str {
            data: Bytes::new(),
            embstr: true,
        }
    }
}

impl StrObj {
    /// `object.c:tryObjectEncoding()`: prefer `int`, then `embstr`, then `raw`.
    pub fn from_bytes(data: Bytes) -> Self {
        if data.len() <= INT_ENCODING_MAX_LEN
            && let Some(v) = strnum::string2ll(&data)
        {
            return StrObj::Int(v);
        }
        let embstr = data.len() <= EMBSTR_LIMIT;
        StrObj::Str { data, embstr }
    }

    /// Build a string object that is explicitly *not* int-encoded. `APPEND`,
    /// `SETRANGE` and `GETSET` results take this path in Redis.
    pub fn raw(data: Bytes) -> Self {
        StrObj::Str {
            data,
            embstr: false,
        }
    }

    #[inline]
    pub fn from_i64(v: i64) -> Self {
        StrObj::Int(v)
    }

    /// The bytes, when they exist verbatim. `None` for the `int` encoding --
    /// callers that need a reply should use `ReplyWriter::bulk_i64`, and
    /// callers that need the bytes should use [`StrObj::to_bytes`] and accept
    /// the allocation.
    #[inline]
    pub fn as_slice(&self) -> Option<&[u8]> {
        match self {
            StrObj::Int(_) => None,
            StrObj::Str { data, .. } => Some(data),
        }
    }

    /// The integer value, whether it is stored as one or merely looks like one.
    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            StrObj::Int(v) => Some(*v),
            StrObj::Str { data, .. } => strnum::string2ll(data),
        }
    }

    /// The value as a float, using Redis's `getDoubleFromObject` rules.
    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            StrObj::Int(v) => Some(*v as f64),
            StrObj::Str { data, .. } => strnum::string2d(data),
        }
    }

    /// Materialise the value. Allocates for the `int` encoding.
    pub fn to_bytes(&self) -> Bytes {
        match self {
            StrObj::Int(v) => {
                let mut buf = itoa::Buffer::new();
                Bytes::copy_from_slice(buf.format(*v).as_bytes())
            }
            StrObj::Str { data, .. } => data.clone(),
        }
    }

    /// `STRLEN`.
    pub fn len(&self) -> usize {
        match self {
            StrObj::Int(v) => {
                let mut buf = itoa::Buffer::new();
                buf.format(*v).len()
            }
            StrObj::Str { data, .. } => data.len(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encoding(&self) -> &'static str {
        match self {
            StrObj::Int(_) => "int",
            StrObj::Str { embstr: true, .. } => "embstr",
            StrObj::Str { embstr: false, .. } => "raw",
        }
    }

    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>()
            + match self {
                StrObj::Int(_) => 0,
                StrObj::Str { data, .. } => data.len(),
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodings_match_redis() {
        assert_eq!(
            StrObj::from_bytes(Bytes::from_static(b"123")).encoding(),
            "int"
        );
        assert_eq!(
            StrObj::from_bytes(Bytes::from_static(b"0")).encoding(),
            "int"
        );
        assert_eq!(
            StrObj::from_bytes(Bytes::from_static(b"-1")).encoding(),
            "int"
        );
        // Leading zeros are not an integer to Redis.
        assert_eq!(
            StrObj::from_bytes(Bytes::from_static(b"0123")).encoding(),
            "embstr"
        );
        assert_eq!(
            StrObj::from_bytes(Bytes::from_static(b"hello")).encoding(),
            "embstr"
        );
        assert_eq!(
            StrObj::from_bytes(Bytes::from_static(b"")).encoding(),
            "embstr"
        );

        let forty_four = Bytes::from(vec![b'a'; 44]);
        assert_eq!(StrObj::from_bytes(forty_four).encoding(), "embstr");
        let forty_five = Bytes::from(vec![b'a'; 45]);
        assert_eq!(StrObj::from_bytes(forty_five).encoding(), "raw");

        // A 21-digit number is too long for the int encoding.
        let big = Bytes::from_static(b"123456789012345678901");
        assert_eq!(StrObj::from_bytes(big).encoding(), "embstr");
    }

    #[test]
    fn accessors() {
        let o = StrObj::from_bytes(Bytes::from_static(b"42"));
        assert_eq!(o.as_i64(), Some(42));
        assert_eq!(o.as_slice(), None);
        assert_eq!(o.len(), 2);
        assert_eq!(&o.to_bytes()[..], b"42");

        let s = StrObj::from_bytes(Bytes::from_static(b"hi"));
        assert_eq!(s.as_slice(), Some(&b"hi"[..]));
        assert_eq!(s.as_i64(), None);
        assert_eq!(s.len(), 2);
    }
}
