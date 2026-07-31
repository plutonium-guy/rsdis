//! Keyspace notifications.
//!
//! Owned by W1c; do not edit if you are not that agent.
//!
//! F0 defined [`NotifyClass`] in full, because `Ctx::notify` is a frozen
//! signature that mentions it and every wave agent calls it from day one. This
//! module adds the delivery half: turning `(class, event, db, key)` into the
//! two channels Redis publishes on,
//!
//! ```text
//! K   __keyspace@<db>__:<key>     -> <event>
//! E   __keyevent@<db>__:<event>   -> <key>
//! ```
//!
//! # The pub/sub seam
//!
//! Fan-out belongs to W3b (`src/pubsub.rs`), which does not exist yet, and
//! `ServerShared` is frozen so no field can be added for it. The seam is
//! therefore a process-wide [`NotifySink`] that W3b installs once at startup
//! with [`install_sink`]:
//!
//! ```ignore
//! // W3b, during server construction:
//! notify::install_sink(Arc::new(pubsub::KeyspaceNotifier));
//! ```
//!
//! [`NotifySink::publish`] has exactly `PUBLISH`'s signature, so W3b's
//! implementation is a one-liner over its own fan-out and this module never
//! learns anything about the pub/sub registry. Until a sink is installed,
//! `dispatch` builds nothing and costs two bit tests.
//!
//! Everything except the sink is testable today: [`CaptureSink`] records what
//! would have been published.

use std::sync::Arc;

use bitflags::bitflags;
use parking_lot::{Mutex, RwLock};
use smallvec::SmallVec;

use crate::ctx::ServerShared;
use crate::object::Key;

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

// ---------------------------------------------------------------------------
// The pub/sub seam
// ---------------------------------------------------------------------------

/// Where a keyspace notification goes once this module has decided that it
/// should be delivered and has built the channel name.
///
/// **Owner of the implementation: W3b.** The signature is deliberately
/// `PUBLISH`'s, so the implementation is a call into the existing fan-out and
/// nothing about the pub/sub registry leaks into this module.
pub trait NotifySink: Send + Sync {
    /// Publish `message` on `channel`, exactly as `PUBLISH` would, including
    /// pattern subscribers. Returns the number of receivers (unused here, but
    /// it keeps the signature identical to `PUBLISH` so an implementation can
    /// simply forward).
    fn publish(&self, server: &ServerShared, db: usize, channel: &[u8], message: &[u8]) -> usize;
}

/// Installed once at startup. An `RwLock` rather than a `OnceLock` so that
/// tests can swap a capture sink in and out; reads only happen after the two
/// cheap flag tests in [`dispatch`], never on a hot path.
static SINK: RwLock<Option<Arc<dyn NotifySink>>> = RwLock::new(None);

/// Serialises tests that install a sink, because the sink is process-wide.
/// Production code never touches this.
pub static SINK_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Install the delivery sink, returning whatever was installed before.
pub fn install_sink(sink: Arc<dyn NotifySink>) -> Option<Arc<dyn NotifySink>> {
    SINK.write().replace(sink)
}

/// Remove the delivery sink. Notifications become no-ops again.
pub fn clear_sink() -> Option<Arc<dyn NotifySink>> {
    SINK.write().take()
}

/// The installed sink, if any.
#[inline]
pub fn sink() -> Option<Arc<dyn NotifySink>> {
    SINK.read().clone()
}

/// Channel names are `__keyspace@<db>__:<key>`; 96 bytes inline covers every
/// key short enough to matter without touching the allocator.
type ChanBuf = SmallVec<[u8; 96]>;

/// `__keyspace@<db>__:<key>`
fn keyspace_channel(db: usize, key: &[u8]) -> ChanBuf {
    let mut buf = ChanBuf::new();
    buf.extend_from_slice(b"__keyspace@");
    let mut fmt = itoa::Buffer::new();
    buf.extend_from_slice(fmt.format(db).as_bytes());
    buf.extend_from_slice(b"__:");
    buf.extend_from_slice(key);
    buf
}

/// `__keyevent@<db>__:<event>`
fn keyevent_channel(db: usize, event: &str) -> ChanBuf {
    let mut buf = ChanBuf::new();
    buf.extend_from_slice(b"__keyevent@");
    let mut fmt = itoa::Buffer::new();
    buf.extend_from_slice(fmt.format(db).as_bytes());
    buf.extend_from_slice(b"__:");
    buf.extend_from_slice(event.as_bytes());
    buf
}

/// Deliver a keyspace notification.
///
/// `Ctx::notify` (frozen) gates on `class.is_enabled_by(configured)` before
/// calling this, but `Ctx::expire_if_needed` does not, so the gate is repeated
/// here. It is two bit tests, and repeating it is what makes this function
/// safe to call unconditionally from the background expiry and eviction
/// cycles, which have no `Ctx`.
///
/// Publishing happens on the caller's thread. Callers that hold a shard lock
/// **must release it first**: `expire.rs` and `evict.rs` collect their victims
/// under the lock and notify afterwards, so a slow subscriber can never stall
/// a shard.
pub fn dispatch(
    server: &ServerShared,
    configured: NotifyClass,
    class: NotifyClass,
    event: &str,
    db: usize,
    key: &Key,
) {
    if !class.is_enabled_by(configured) {
        return;
    }
    let Some(sink) = sink() else {
        return;
    };
    if configured.contains(NotifyClass::KEYSPACE) {
        let chan = keyspace_channel(db, key);
        sink.publish(server, db, &chan, event.as_bytes());
    }
    if configured.contains(NotifyClass::KEYEVENT) {
        let chan = keyevent_channel(db, event);
        sink.publish(server, db, &chan, key);
    }
}

// ---------------------------------------------------------------------------
// Test sink
// ---------------------------------------------------------------------------

/// A sink that records instead of publishing.
///
/// Not `#[cfg(test)]`: integration tests live in a separate crate and need it
/// too. Take [`SINK_TEST_LOCK`] around any test that installs one.
#[derive(Debug, Default)]
pub struct CaptureSink {
    events: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl CaptureSink {
    pub fn new() -> Self {
        CaptureSink::default()
    }

    /// Every `(channel, message)` seen so far, clearing the log.
    pub fn take(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        core::mem::take(&mut *self.events.lock())
    }

    /// Channels seen so far, as lossy UTF-8, clearing the log.
    pub fn take_strings(&self) -> Vec<(String, String)> {
        self.take()
            .into_iter()
            .map(|(c, m)| {
                (
                    String::from_utf8_lossy(&c).into_owned(),
                    String::from_utf8_lossy(&m).into_owned(),
                )
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.events.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl NotifySink for CaptureSink {
    fn publish(&self, _server: &ServerShared, _db: usize, channel: &[u8], message: &[u8]) -> usize {
        self.events
            .lock()
            .push((channel.to_vec(), message.to_vec()));
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use bytes::Bytes;

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

    #[test]
    fn channel_names_match_redis() {
        assert_eq!(&keyspace_channel(0, b"foo")[..], b"__keyspace@0__:foo");
        assert_eq!(&keyspace_channel(15, b"a:b")[..], b"__keyspace@15__:a:b");
        assert_eq!(
            &keyevent_channel(9, "expired")[..],
            b"__keyevent@9__:expired"
        );
    }

    #[test]
    fn dispatch_publishes_both_channels() {
        let _guard = SINK_TEST_LOCK.lock();
        let server = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let cap = Arc::new(CaptureSink::new());
        install_sink(cap.clone());

        let key = Bytes::from_static(b"mykey");
        let all = NotifyClass::parse("KEA").unwrap();
        dispatch(&server, all, NotifyClass::GENERIC, "del", 3, &key);
        assert_eq!(
            cap.take_strings(),
            vec![
                ("__keyspace@3__:mykey".to_string(), "del".to_string()),
                ("__keyevent@3__:del".to_string(), "mykey".to_string()),
            ]
        );

        // K only.
        let k_only = NotifyClass::parse("Kg").unwrap();
        dispatch(&server, k_only, NotifyClass::GENERIC, "del", 0, &key);
        assert_eq!(
            cap.take_strings(),
            vec![("__keyspace@0__:mykey".to_string(), "del".to_string())]
        );

        // E only.
        let e_only = NotifyClass::parse("Eg").unwrap();
        dispatch(&server, e_only, NotifyClass::GENERIC, "del", 0, &key);
        assert_eq!(
            cap.take_strings(),
            vec![("__keyevent@0__:del".to_string(), "mykey".to_string())]
        );

        // Class not armed: nothing at all.
        dispatch(&server, e_only, NotifyClass::HASH, "hset", 0, &key);
        assert!(cap.is_empty());

        clear_sink();
    }

    #[test]
    fn dispatch_without_a_sink_is_a_no_op() {
        let _guard = SINK_TEST_LOCK.lock();
        clear_sink();
        let server = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        // Must not panic and must not require anything to be installed.
        dispatch(
            &server,
            NotifyClass::parse("KEA").unwrap(),
            NotifyClass::GENERIC,
            "del",
            0,
            &Bytes::from_static(b"k"),
        );
    }
}
