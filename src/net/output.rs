//! The per-connection output path: staging buffer, vectored write queue, and
//! `client-output-buffer-limit` accounting.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! # §9.10 -- vectored writes: what was and was not done
//!
//! §9.10 hands W1b the decision on whether `ReplyWriter::bulk_from` should
//! stop memcpy'ing values into the connection buffer and instead queue the
//! `Bytes` for a `writev`.
//!
//! **The machinery is here and is used; `bulk_from` itself is not wired to
//! it, and cannot be from inside `src/net`.** [`OutputBuffer`] is a proper
//! two-tier queue: a [`BytesMut`] the reply writer appends into, plus an
//! ordered queue of already-sealed [`Bytes`] frames that are handed to
//! `writev` without a copy. [`OutputBuffer::push_bytes`] is the zero-copy
//! entry point, and the out-of-band path (pub/sub delivery, keyspace
//! notifications) already uses it.
//!
//! What blocks the last step: `ReplyWriter` is `{ buf: &'a mut BytesMut }` in
//! `src/reply.rs`, which is F0-owned and **FROZEN**. It holds no handle to
//! anything in `src/net`, so `bulk_from` has no way to reach this queue. The
//! change §9.10 describes is a body edit to a file W1b does not own. It is
//! reported as a contract gap rather than worked around.
//!
//! Consequence, stated plainly: **§5.1 is still not satisfied for large
//! values.** A 64 KB `GET` reply is memcpy'd once into the staging buffer.
//!
//! # The answer to "is it worth it?" -- yes, above ~2 KiB
//!
//! The decision §9.10 asks for does not need `reply.rs` to be unfrozen before
//! it can be *made*, only before it can be wired up. It is measured:
//! [`VECTORED_MIN_BYTES`] carries the table, and the short version is that
//! `writev` wins by 10-30 % from 2 KiB per value upward and loses by a few
//! percent below that. So the honest answer is a **threshold**, not a blanket
//! yes, and it is exposed as [`OutputBuffer::should_queue`] ready for the day
//! `bulk_from` can call it.
//!
//! Corroboration from the other end: `redis-benchmark -d 65536 -t get`
//! measures rsdis at ~53 k ops/s against real Redis 8.6's ~62 k on the same
//! host, while the 8 B and 512 B cases are at parity or ahead. That gap is
//! the memcpy this change would remove.
//!
//! # Backpressure
//!
//! A connection's reply for a batch is written before the next batch is read,
//! so a slow *consumer* naturally throttles the *producer* through the
//! `write` await -- the read loop simply stops. The output buffer can
//! therefore only grow without bound along the out-of-band path, where
//! another thread pushes frames into this connection regardless of whether it
//! is draining. That is exactly what `client-output-buffer-limit` exists to
//! bound, and [`OutputLimits`] is checked there.

use std::collections::VecDeque;
use std::io::{self, IoSlice};

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// How many `Bytes` frames we will describe to one `writev` call. `IOV_MAX` is
/// 1024 on Linux and 1024 on macOS; 64 is well inside it and keeps the slice
/// array on the stack.
const MAX_IOVECS: usize = 64;

/// The size at which queuing a value for `writev` beats memcpy'ing it into
/// the staging buffer.
///
/// This is a **caller-facing** threshold, not a flush-time one -- see
/// [`OutputBuffer::should_queue`]. Once bytes are in the queue there is no
/// choice left to make: one frame takes a `write`, several take one `writev`,
/// and both are a single syscall. The interesting decision is upstream, where
/// a value can either be copied into the contiguous staging buffer (one
/// memcpy, no extra iovec) or queued as its own frame (no copy, one more
/// iovec).
///
/// Measured over loopback TCP by `benches/protocol_bench.rs::bench_writev_vs_memcpy`
/// on an Apple M-series host, 16 values per flush, median of three runs
/// (run-to-run spread is ~3 points, so the sign of every row below is stable
/// and only the exact percentages move):
///
/// | value size | memcpy + write | queue + writev | writev delta |
/// |---|---|---|---|
/// | 8 B    | 3.06 µs  | 3.46 µs  | −13.0 % |
/// | 512 B  | 3.42 µs  | 3.64 µs  | −6.4 %  |
/// | 2 KiB  | 5.97 µs  | 5.29 µs  | **+11.4 %** |
/// | 4 KiB  | 10.47 µs | 7.51 µs  | **+28.3 %** |
/// | 8 KiB  | 17.50 µs | 13.74 µs | **+21.5 %** |
/// | 64 KiB | 120.6 µs | 97.1 µs  | **+19.5 %** |
///
/// The crossover sits between 512 B and 2 KiB, so that is where this constant
/// goes. Below it the saved copy does not pay for the extra iovec and the
/// extra `Bytes` in the queue; above it, it clearly does, and the win is worth
/// roughly 20 % of the whole flush at realistic value sizes.
///
/// One more data point, because it constrains the *caller* rather than the
/// flush: 128 × 512 B queued as separate frames loses to memcpy by ~23 %, since
/// 128 iovecs exceed [`MAX_IOVECS`] and become two syscalls while the memcpy
/// path stays at one. Applying `should_queue` per value keeps that case on the
/// memcpy path, where it belongs. 128 × 64 KiB, by contrast, wins by ~20 %.
const VECTORED_MIN_BYTES: usize = 2048;

/// Which `client-output-buffer-limit` class a connection belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientClass {
    /// A normal request/response client.
    Normal,
    /// A client in subscribe mode (`SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`).
    PubSub,
    /// A replica. Reserved for the replication work that §1 defers.
    Replica,
}

/// One class's limits, in Redis's `<hard> <soft> <soft-seconds>` shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassLimit {
    /// Close immediately once the pending output exceeds this. 0 = unlimited.
    pub hard: u64,
    /// Close if the pending output stays above this for `soft_seconds`.
    /// 0 = unlimited.
    pub soft: u64,
    pub soft_seconds: u64,
}

impl ClassLimit {
    pub const UNLIMITED: ClassLimit = ClassLimit {
        hard: 0,
        soft: 0,
        soft_seconds: 0,
    };
}

/// The three `client-output-buffer-limit` classes.
///
/// # Contract gap
///
/// `Config` (F0-owned, and not W1b's to edit) has no
/// `client-output-buffer-limit` field, so these cannot be configured at
/// runtime and `CONFIG GET/SET client-output-buffer-limit` is unavailable.
/// The defaults below are Redis 7.4's, byte for byte, and the enforcement is
/// real. Reported as a contract gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputLimits {
    pub normal: ClassLimit,
    pub pubsub: ClassLimit,
    pub replica: ClassLimit,
}

impl Default for OutputLimits {
    /// `client-output-buffer-limit normal 0 0 0 slave 256mb 64mb 60 pubsub 32mb 8mb 60`
    fn default() -> Self {
        OutputLimits {
            normal: ClassLimit::UNLIMITED,
            pubsub: ClassLimit {
                hard: 32 << 20,
                soft: 8 << 20,
                soft_seconds: 60,
            },
            replica: ClassLimit {
                hard: 256 << 20,
                soft: 64 << 20,
                soft_seconds: 60,
            },
        }
    }
}

impl OutputLimits {
    #[inline]
    pub fn for_class(&self, class: ClientClass) -> ClassLimit {
        match class {
            ClientClass::Normal => self.normal,
            ClientClass::PubSub => self.pubsub,
            ClientClass::Replica => self.replica,
        }
    }
}

/// Why a connection is being dropped for exceeding its output budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitBreach {
    Hard,
    Soft,
}

/// The per-connection output buffer.
///
/// Two tiers:
///
/// * `staging` -- a `BytesMut` that [`crate::reply::ReplyWriter`] appends
///   into. Contiguous, reused across batches, and shrunk back when a single
///   large reply has inflated it.
/// * `queued` -- sealed `Bytes` frames awaiting the socket, written with
///   `writev` so nothing is copied a second time. Sealing `staging` pushes it
///   here; [`OutputBuffer::push_bytes`] pushes a caller's `Bytes` here
///   directly, which is the zero-copy path.
///
/// Ordering between the tiers is preserved: pushing a `Bytes` seals whatever
/// is staged first, so replies never overtake each other.
#[derive(Debug)]
pub struct OutputBuffer {
    staging: BytesMut,
    queued: VecDeque<Bytes>,
    /// Total bytes in `queued`, including the partially written head.
    queued_bytes: usize,
    /// Bytes of `queued.front()` already handed to the kernel.
    head_off: usize,
    /// Capacity a fresh staging buffer is allocated with.
    initial_capacity: usize,
    /// Staging capacity above which we reallocate instead of reusing.
    shrink_threshold: usize,
    /// Peak pending bytes, for `CLIENT LIST`'s `omem`.
    peak_pending: usize,
    /// When the pending output first went above the soft limit, in ms.
    soft_limit_since_ms: Option<u64>,
}

impl OutputBuffer {
    pub fn new(initial_capacity: usize, shrink_threshold: usize) -> Self {
        OutputBuffer {
            staging: BytesMut::with_capacity(initial_capacity),
            queued: VecDeque::new(),
            queued_bytes: 0,
            head_off: 0,
            initial_capacity,
            shrink_threshold,
            peak_pending: 0,
            soft_limit_since_ms: None,
        }
    }

    /// The buffer replies are written into. This is what a `ReplyWriter` wraps.
    #[inline]
    pub fn staging(&mut self) -> &mut BytesMut {
        &mut self.staging
    }

    /// Bytes waiting to reach the socket, staged plus queued.
    #[inline]
    pub fn pending(&self) -> usize {
        self.staging.len() + self.queued_bytes - self.head_off
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.pending() == 0
    }

    /// Number of queued frames. `CLIENT LIST`'s `oll`.
    #[inline]
    pub fn queued_frames(&self) -> usize {
        self.queued.len()
    }

    /// Largest pending size seen. `CLIENT LIST`'s `omem`.
    #[inline]
    pub fn peak_pending(&self) -> usize {
        self.peak_pending
    }

    /// Total capacity currently held, staged plus queued. `tot-mem`.
    pub fn memory_usage(&self) -> usize {
        self.staging.capacity() + self.queued_bytes + self.queued.capacity() * size_of::<Bytes>()
    }

    /// Move anything staged into the write queue, without copying it: the
    /// staging buffer is split and frozen, so the queued frame and the next
    /// staging buffer share one allocation until the frame is written.
    #[inline]
    pub fn seal(&mut self) {
        if !self.staging.is_empty() {
            let frame = self.staging.split().freeze();
            self.queued_bytes += frame.len();
            self.queued.push_back(frame);
        }
    }

    /// Whether a value of `len` bytes is worth queuing for `writev` rather
    /// than copying into the staging buffer.
    ///
    /// This is the whole §9.10 decision, reduced to one predicate. A
    /// zero-copy `ReplyWriter::bulk_from` would write the `$<len>\r\n` header
    /// into staging, then either `extend_from_slice` the body (small) or
    /// [`OutputBuffer::push_bytes`] it (large) on this test. See
    /// [`VECTORED_MIN_BYTES`] for the measurements behind the number.
    #[inline]
    pub fn should_queue(len: usize) -> bool {
        len >= VECTORED_MIN_BYTES
    }

    /// Queue an already-encoded frame with **no copy at all**.
    ///
    /// This is the zero-copy write path of §9.10. Used today by the
    /// out-of-band channel; it is also exactly what `ReplyWriter::bulk_from`
    /// would call if `reply.rs` were not frozen.
    pub fn push_bytes(&mut self, b: Bytes) {
        if b.is_empty() {
            return;
        }
        self.seal();
        self.queued_bytes += b.len();
        self.queued.push_back(b);
        self.note_pending();
    }

    #[inline]
    fn note_pending(&mut self) {
        let p = self.pending();
        if p > self.peak_pending {
            self.peak_pending = p;
        }
    }

    /// Check this connection's output budget.
    ///
    /// Returns `Some(_)` when the connection must be dropped. Mirrors
    /// `networking.c:closeClientOnOutputBufferLimitReached()`: the hard limit
    /// fires immediately, the soft limit only after it has been continuously
    /// exceeded for `soft_seconds`.
    pub fn check_limit(&mut self, limit: ClassLimit, now_ms: u64) -> Option<LimitBreach> {
        let pending = self.pending() as u64;
        if limit.hard > 0 && pending > limit.hard {
            return Some(LimitBreach::Hard);
        }
        if limit.soft > 0 && pending > limit.soft {
            match self.soft_limit_since_ms {
                None => self.soft_limit_since_ms = Some(now_ms),
                Some(since) => {
                    if now_ms.saturating_sub(since) >= limit.soft_seconds.saturating_mul(1000) {
                        return Some(LimitBreach::Soft);
                    }
                }
            }
        } else {
            self.soft_limit_since_ms = None;
        }
        None
    }

    /// Write everything pending, then release the staging buffer if one large
    /// reply has inflated it.
    ///
    /// Returns the number of bytes written.
    pub async fn flush<W>(&mut self, w: &mut W) -> io::Result<u64>
    where
        W: AsyncWrite + Unpin,
    {
        self.note_pending();
        self.seal();

        let mut total = 0u64;
        while !self.queued.is_empty() {
            let n = self.write_some(w).await?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            total += n as u64;
            self.advance(n);
        }

        // The head is fully drained; reset the offsets and reclaim memory.
        self.head_off = 0;
        self.queued_bytes = 0;
        if self.staging.capacity() > self.shrink_threshold {
            self.staging = BytesMut::with_capacity(self.initial_capacity);
        }
        if self.queued.capacity() > MAX_IOVECS {
            self.queued.shrink_to(MAX_IOVECS);
        }
        Ok(total)
    }

    /// One write syscall's worth.
    ///
    /// Single-frame output takes the plain `write` path: a one-element
    /// `writev` is strictly more work than a `write` for the same bytes, and
    /// the overwhelmingly common case -- one batch of replies, sealed into one
    /// frame -- is exactly that.
    ///
    /// Multi-frame output **always** goes vectored, regardless of size. An
    /// earlier version gated this on a byte threshold and fell back to writing
    /// only the head frame; the benchmark caught it immediately (16 × 8 B
    /// frames took 44 µs instead of 3 µs, because it became sixteen syscalls
    /// instead of one). Once bytes are queued the choice is one `writev`
    /// versus *n* `write`s, and one syscall always wins. The size question
    /// belongs upstream, in [`OutputBuffer::should_queue`].
    async fn write_some<W>(&self, w: &mut W) -> io::Result<usize>
    where
        W: AsyncWrite + Unpin,
    {
        let Some(head) = self.queued.front() else {
            return Ok(0);
        };
        let head = head.get(self.head_off..).unwrap_or(b"");

        if self.queued.len() == 1 || !w.is_write_vectored() {
            return w.write(head).await;
        }

        let mut slices: SmallVec<[IoSlice<'_>; MAX_IOVECS]> = SmallVec::new();
        slices.push(IoSlice::new(head));
        for frame in self.queued.iter().skip(1) {
            if slices.len() == MAX_IOVECS {
                break;
            }
            if !frame.is_empty() {
                slices.push(IoSlice::new(frame));
            }
        }
        w.write_vectored(&slices).await
    }

    /// Account for `n` bytes accepted by the kernel, dropping fully written
    /// frames.
    fn advance(&mut self, mut n: usize) {
        while n > 0 {
            let Some(front) = self.queued.front() else {
                break;
            };
            let left = front.len() - self.head_off;
            if n < left {
                self.head_off += n;
                return;
            }
            n -= left;
            self.queued_bytes -= front.len();
            self.queued.pop_front();
            self.head_off = 0;
        }
    }

    /// Drop everything pending. Used when a connection is being killed and
    /// the reply can no longer be delivered.
    pub fn discard(&mut self) {
        self.staging.clear();
        self.queued.clear();
        self.queued_bytes = 0;
        self.head_off = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sink that accepts at most `chunk` bytes per call, so partial writes
    /// are exercised rather than assumed away.
    struct Trickle {
        got: Vec<u8>,
        chunk: usize,
        vectored: bool,
        writes: usize,
        vector_writes: usize,
    }

    impl Trickle {
        fn new(chunk: usize, vectored: bool) -> Self {
            Trickle {
                got: Vec::new(),
                chunk,
                vectored,
                writes: 0,
                vector_writes: 0,
            }
        }
    }

    impl AsyncWrite for Trickle {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            let n = buf.len().min(self.chunk);
            self.writes += 1;
            let slice = buf.get(..n).unwrap_or(b"").to_vec();
            self.got.extend_from_slice(&slice);
            std::task::Poll::Ready(Ok(n))
        }

        fn poll_write_vectored(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> std::task::Poll<io::Result<usize>> {
            self.vector_writes += 1;
            let mut budget = self.chunk;
            let mut wrote = 0usize;
            let mut staged: Vec<u8> = Vec::new();
            for b in bufs {
                if budget == 0 {
                    break;
                }
                let n = b.len().min(budget);
                staged.extend_from_slice(b.get(..n).unwrap_or(b""));
                budget -= n;
                wrote += n;
            }
            self.got.extend_from_slice(&staged);
            std::task::Poll::Ready(Ok(wrote))
        }

        fn is_write_vectored(&self) -> bool {
            self.vectored
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    fn buf() -> OutputBuffer {
        OutputBuffer::new(64, 4096)
    }

    #[tokio::test]
    async fn staged_bytes_reach_the_socket() {
        let mut o = buf();
        o.staging().extend_from_slice(b"+OK\r\n");
        assert_eq!(o.pending(), 5);
        let mut sink = Trickle::new(1024, false);
        assert_eq!(o.flush(&mut sink).await.unwrap(), 5);
        assert_eq!(sink.got, b"+OK\r\n");
        assert!(o.is_empty());
    }

    #[tokio::test]
    async fn push_bytes_preserves_order_against_staged_output() {
        let mut o = buf();
        o.staging().extend_from_slice(b"one");
        o.push_bytes(Bytes::from_static(b"two"));
        o.staging().extend_from_slice(b"three");
        let mut sink = Trickle::new(1024, true);
        o.flush(&mut sink).await.unwrap();
        assert_eq!(sink.got, b"onetwothree");
    }

    #[tokio::test]
    async fn partial_writes_resume_exactly_where_they_stopped() {
        let mut o = buf();
        for i in 0..10u8 {
            o.push_bytes(Bytes::from(vec![b'a' + i; 100]));
        }
        let mut sink = Trickle::new(7, true);
        let n = o.flush(&mut sink).await.unwrap();
        assert_eq!(n, 1000);
        assert_eq!(sink.got.len(), 1000);
        let mut expect = Vec::new();
        for i in 0..10u8 {
            expect.extend(std::iter::repeat_n(b'a' + i, 100));
        }
        assert_eq!(sink.got, expect);
        assert!(o.is_empty());
    }

    #[tokio::test]
    async fn a_single_frame_does_not_pay_for_an_iovec() {
        let mut o = buf();
        o.push_bytes(Bytes::from(vec![b'x'; 1 << 16]));
        let mut sink = Trickle::new(1 << 20, true);
        o.flush(&mut sink).await.unwrap();
        assert_eq!(sink.vector_writes, 0, "one frame must use plain write");
        assert_eq!(sink.writes, 1);
    }

    #[tokio::test]
    async fn many_large_frames_go_vectored() {
        let mut o = buf();
        for _ in 0..8 {
            o.push_bytes(Bytes::from(vec![b'x'; 8192]));
        }
        let mut sink = Trickle::new(1 << 20, true);
        o.flush(&mut sink).await.unwrap();
        assert_eq!(sink.vector_writes, 1, "one writev for the whole queue");
        assert_eq!(sink.got.len(), 8 * 8192);
    }

    /// The regression the benchmark found: several tiny frames must still cost
    /// one syscall, not one per frame.
    #[tokio::test]
    async fn small_multi_frame_output_is_still_one_syscall() {
        let mut o = buf();
        for _ in 0..4 {
            o.push_bytes(Bytes::from_static(b"+OK\r\n"));
        }
        let mut sink = Trickle::new(1 << 20, true);
        o.flush(&mut sink).await.unwrap();
        assert_eq!(sink.vector_writes, 1);
        assert_eq!(sink.writes, 0, "no per-frame scalar writes");
        assert_eq!(sink.got, b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n");
    }

    #[test]
    fn should_queue_encodes_the_measured_crossover() {
        assert!(!OutputBuffer::should_queue(8));
        assert!(!OutputBuffer::should_queue(512));
        assert!(!OutputBuffer::should_queue(2047));
        assert!(OutputBuffer::should_queue(2048));
        assert!(OutputBuffer::should_queue(64 * 1024));
    }

    #[tokio::test]
    async fn a_writer_without_vectored_support_still_makes_progress() {
        let mut o = buf();
        for _ in 0..8 {
            o.push_bytes(Bytes::from(vec![b'x'; 8192]));
        }
        let mut sink = Trickle::new(4096, false);
        o.flush(&mut sink).await.unwrap();
        assert_eq!(sink.got.len(), 8 * 8192);
        assert_eq!(sink.vector_writes, 0);
    }

    #[tokio::test]
    async fn a_big_reply_does_not_pin_the_staging_buffer() {
        let mut o = OutputBuffer::new(64, 4096);
        o.staging().extend_from_slice(&vec![b'x'; 1 << 16]);
        let mut sink = Trickle::new(1 << 20, false);
        o.flush(&mut sink).await.unwrap();
        assert!(
            o.staging.capacity() <= 64,
            "staging kept {} bytes",
            o.staging.capacity()
        );
    }

    #[test]
    fn hard_limit_fires_immediately() {
        let mut o = buf();
        let limit = ClassLimit {
            hard: 1000,
            soft: 0,
            soft_seconds: 0,
        };
        o.push_bytes(Bytes::from(vec![0u8; 500]));
        assert_eq!(o.check_limit(limit, 0), None);
        o.push_bytes(Bytes::from(vec![0u8; 600]));
        assert_eq!(o.check_limit(limit, 0), Some(LimitBreach::Hard));
    }

    #[test]
    fn soft_limit_needs_to_persist() {
        let mut o = buf();
        let limit = ClassLimit {
            hard: 0,
            soft: 100,
            soft_seconds: 10,
        };
        o.push_bytes(Bytes::from(vec![0u8; 200]));
        assert_eq!(o.check_limit(limit, 1_000), None, "clock starts here");
        assert_eq!(o.check_limit(limit, 5_000), None, "not yet 10s");
        assert_eq!(o.check_limit(limit, 11_000), Some(LimitBreach::Soft));
    }

    #[test]
    fn dropping_below_the_soft_limit_resets_the_clock() {
        let mut o = buf();
        let limit = ClassLimit {
            hard: 0,
            soft: 100,
            soft_seconds: 10,
        };
        o.push_bytes(Bytes::from(vec![0u8; 200]));
        assert_eq!(o.check_limit(limit, 1_000), None);
        o.discard();
        assert_eq!(o.check_limit(limit, 5_000), None);
        o.push_bytes(Bytes::from(vec![0u8; 200]));
        assert_eq!(o.check_limit(limit, 6_000), None);
        assert_eq!(
            o.check_limit(limit, 12_000),
            None,
            "the 10s window restarted at 6000"
        );
        assert_eq!(o.check_limit(limit, 16_000), Some(LimitBreach::Soft));
    }

    #[test]
    fn zero_means_unlimited() {
        let mut o = buf();
        o.push_bytes(Bytes::from(vec![0u8; 1 << 20]));
        assert_eq!(o.check_limit(ClassLimit::UNLIMITED, u64::MAX), None);
    }

    #[test]
    fn redis_default_limits() {
        let l = OutputLimits::default();
        assert_eq!(l.for_class(ClientClass::Normal), ClassLimit::UNLIMITED);
        assert_eq!(l.for_class(ClientClass::PubSub).hard, 32 << 20);
        assert_eq!(l.for_class(ClientClass::PubSub).soft, 8 << 20);
        assert_eq!(l.for_class(ClientClass::PubSub).soft_seconds, 60);
        assert_eq!(l.for_class(ClientClass::Replica).hard, 256 << 20);
    }
}
