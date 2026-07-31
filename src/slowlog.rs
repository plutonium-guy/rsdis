//! `SLOWLOG`.
//!
//! Owned by W3c; do not edit if you are not that agent.
//!
//! F0 declares [`SlowLog`] because it hangs off `ServerShared`, which lives in
//! the frozen `src/ctx.rs`. W3c fills in the ring buffer, the
//! `slowlog-log-slower-than` / `slowlog-max-len` handling and the reply
//! shapes for `SLOWLOG GET/LEN/RESET/HELP`.

/// Bounded ring of slow command records.
#[derive(Debug, Default)]
pub struct SlowLog {
    _placeholder: (),
}

impl SlowLog {
    pub fn new() -> Self {
        SlowLog::default()
    }

    /// `SLOWLOG LEN`.
    pub fn len(&self) -> usize {
        0
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
