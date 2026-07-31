//! Keyspace notifications.
//!
//! Owned by W1c; do not edit if you are not that agent.
//!
//! F0 defines [`NotifyClass`] in full, because `Ctx::notify` is a frozen
//! signature that mentions it and every wave agent will call it from day one.
//! The delivery half -- publishing `__keyspace@<db>__:<key>` and
//! `__keyevent@<db>__:<event>` through pub/sub -- is W1c's, on top of W3b's
//! pub/sub registry.

use bitflags::bitflags;

bitflags! {
    /// `notify-keyspace-events` classes, with Redis's exact character codes.
    ///
    /// From `notify.c:keyspaceEventsStringToFlags()`:
    ///
    /// ```text
    /// K  Keyspace events, published to __keyspace@<db>__ prefix
    /// E  Keyevent events, published to __keyevent@<db>__ prefix
    /// g  Generic commands (DEL, EXPIRE, RENAME, ...)
    /// $  String commands
    /// l  List commands
    /// s  Set commands
    /// h  Hash commands
    /// z  Sorted set commands
    /// x  Expired events
    /// e  Evicted events
    /// n  New key events (not included in A)
    /// t  Stream commands
    /// d  Module key type events
    /// m  Key-miss events (not included in A)
    /// A  Alias for "g$lshzxetd"
    /// ```
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
    pub struct NotifyClass: u32 {
        const KEYSPACE  = 1 << 0;   // K
        const KEYEVENT  = 1 << 1;   // E
        const GENERIC   = 1 << 2;   // g
        const STRING    = 1 << 3;   // $
        const LIST      = 1 << 4;   // l
        const SET       = 1 << 5;   // s
        const HASH      = 1 << 6;   // h
        const ZSET      = 1 << 7;   // z
        const EXPIRED   = 1 << 8;   // x
        const EVICTED   = 1 << 9;   // e
        const STREAM    = 1 << 10;  // t
        const KEY_MISS  = 1 << 11;  // m
        const NEW       = 1 << 12;  // n
        const MODULE    = 1 << 13;  // d

        /// `A` -- everything except `K`, `E`, `m` and `n`.
        const ALL = Self::GENERIC.bits()
                  | Self::STRING.bits()
                  | Self::LIST.bits()
                  | Self::SET.bits()
                  | Self::HASH.bits()
                  | Self::ZSET.bits()
                  | Self::EXPIRED.bits()
                  | Self::EVICTED.bits()
                  | Self::STREAM.bits()
                  | Self::MODULE.bits();
    }
}

impl NotifyClass {
    /// Parse a `notify-keyspace-events` string. Returns `None` on an unknown
    /// character, which `CONFIG SET` reports as an error.
    pub fn parse(s: &str) -> Option<NotifyClass> {
        let mut flags = NotifyClass::empty();
        for c in s.chars() {
            flags |= match c {
                'K' => NotifyClass::KEYSPACE,
                'E' => NotifyClass::KEYEVENT,
                'g' => NotifyClass::GENERIC,
                '$' => NotifyClass::STRING,
                'l' => NotifyClass::LIST,
                's' => NotifyClass::SET,
                'h' => NotifyClass::HASH,
                'z' => NotifyClass::ZSET,
                'x' => NotifyClass::EXPIRED,
                'e' => NotifyClass::EVICTED,
                't' => NotifyClass::STREAM,
                'm' => NotifyClass::KEY_MISS,
                'n' => NotifyClass::NEW,
                'd' => NotifyClass::MODULE,
                'A' => NotifyClass::ALL,
                _ => return None,
            };
        }
        Some(flags)
    }

    /// Render back to the config string, matching
    /// `notify.c:keyspaceEventsFlagsToString()`: the `A` alias is emitted when
    /// every class it covers is present.
    pub fn to_config_string(self) -> String {
        let mut s = String::new();
        if self.contains(NotifyClass::ALL) {
            s.push('A');
        } else {
            for (flag, ch) in [
                (NotifyClass::GENERIC, 'g'),
                (NotifyClass::STRING, '$'),
                (NotifyClass::LIST, 'l'),
                (NotifyClass::SET, 's'),
                (NotifyClass::HASH, 'h'),
                (NotifyClass::ZSET, 'z'),
                (NotifyClass::EXPIRED, 'x'),
                (NotifyClass::EVICTED, 'e'),
                (NotifyClass::STREAM, 't'),
                (NotifyClass::MODULE, 'd'),
            ] {
                if self.contains(flag) {
                    s.push(ch);
                }
            }
        }
        if self.contains(NotifyClass::KEY_MISS) {
            s.push('m');
        }
        if self.contains(NotifyClass::NEW) {
            s.push('n');
        }
        if self.contains(NotifyClass::KEYSPACE) {
            s.push('K');
        }
        if self.contains(NotifyClass::KEYEVENT) {
            s.push('E');
        }
        s
    }

    /// True when an event of this class should actually be delivered under the
    /// configured mask. Both a class bit and at least one of `K`/`E` must be
    /// set, which is why misconfigured `notify-keyspace-events` silently
    /// delivers nothing in real Redis too.
    #[inline]
    pub fn is_enabled_by(self, configured: NotifyClass) -> bool {
        configured.intersects(NotifyClass::KEYSPACE | NotifyClass::KEYEVENT)
            && configured.intersects(self)
    }
}

/// Deliver a keyspace notification.
///
/// F0 seeds the signature and the (already-gated) call site in `Ctx::notify`
/// so that every wave agent can emit events from day one. W1c implements the
/// body: publish `__keyspace@<db>__:<key>` -> `<event>` when `K` is set, and
/// `__keyevent@<db>__:<event>` -> `<key>` when `E` is set, both through W3b's
/// pub/sub fan-out.
///
/// The caller has already checked `class.is_enabled_by(configured)`, so this
/// only runs when at least one subscriber class is armed.
#[inline]
pub fn dispatch(
    _server: &crate::ctx::ServerShared,
    _configured: NotifyClass,
    _class: NotifyClass,
    _event: &str,
    _db: usize,
    _key: &crate::object::Key,
) {
    // Owner: W1c.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trip() {
        assert_eq!(NotifyClass::parse("KEA").unwrap().to_config_string(), "AKE");
        assert_eq!(NotifyClass::parse("").unwrap(), NotifyClass::empty());
        assert_eq!(NotifyClass::parse("Elg").unwrap().to_config_string(), "glE");
        assert!(NotifyClass::parse("Q").is_none());
    }

    #[test]
    fn a_excludes_key_miss_and_new() {
        let a = NotifyClass::parse("A").unwrap();
        assert!(!a.contains(NotifyClass::KEY_MISS));
        assert!(!a.contains(NotifyClass::NEW));
        assert!(a.contains(NotifyClass::GENERIC));
    }

    #[test]
    fn delivery_requires_k_or_e() {
        let cfg = NotifyClass::parse("gl").unwrap();
        assert!(!NotifyClass::GENERIC.is_enabled_by(cfg));
        let cfg = NotifyClass::parse("Kgl").unwrap();
        assert!(NotifyClass::GENERIC.is_enabled_by(cfg));
        assert!(!NotifyClass::HASH.is_enabled_by(cfg));
    }
}
