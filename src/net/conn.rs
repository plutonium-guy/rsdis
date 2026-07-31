//! The per-connection read/execute/flush loop.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! # Pipelining (§2.2)
//!
//! One iteration = one `read()`, then *every* complete command in the buffer
//! is executed into a single output buffer, then *one* flush. A client that
//! pipelines 1000 `SET`s pays one syscall each way, not 1000.
//!
//! # Locks and `.await`
//!
//! `engine::dispatch` is synchronous and releases every shard guard before it
//! returns, so no `parking_lot` guard is ever held across a suspension point.
//! That is load-bearing for the deadlock argument in `engine.rs`; keep it that
//! way.
//!
//! # Buffers
//!
//! The read buffer starts at [`READ_BUF`] and grows on demand. It is released
//! back to [`READ_BUF`] once it is empty, no frame is half-parsed, and it has
//! grown past [`BUF_SHRINK_THRESHOLD`] -- so a client that once sent a 10 MB
//! value does not hold 10 MB for the rest of its life. The same rule applies
//! to the output staging buffer, inside [`OutputBuffer`].
//!
//! Shrinking is gated on `RequestParser::is_mid_frame()`: the parser's
//! recorded offsets are relative to the buffer's origin, so reallocating under
//! a partial frame would be correct only because the origin does not move --
//! but reallocating *while a large bulk is arriving* would also undo the
//! growth we are about to need again. Both reasons point the same way.
//!
//! # Backpressure
//!
//! The loop reads, executes and flushes in sequence: nothing is read while a
//! reply is still going out, so a slow consumer stalls its own producer
//! through the `write` await. The only way output can outrun the socket is the
//! out-of-band channel, where another thread pushes into this connection
//! regardless of whether it is draining -- and that is bounded by
//! `client-output-buffer-limit` (see [`crate::net::output`]).

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, trace};

use crate::ctx::{ClientFlags, ClientHandle, ClientState, OutOfBand, ServerShared};
use crate::engine::{self, Outcome};
use crate::info::Stats;
use crate::net::output::{ClientClass, LimitBreach, OutputBuffer, OutputLimits};
use crate::net::registry::{self, ConnEntry, ConnSnapshot};
use crate::reply::ReplyWriter;
use crate::resp::parser::{Parsed, RequestParser};

/// Initial read buffer. Grows on demand; 16 KiB covers a typical pipelined
/// burst without a realloc.
pub const READ_BUF: usize = 16 * 1024;
/// Shrink the buffers back down once they exceed this and are empty, so one
/// large request does not pin memory for the connection's lifetime.
pub const BUF_SHRINK_THRESHOLD: usize = 256 * 1024;
/// How often the parser re-reads `proto-max-bulk-len`, in read batches.
const CONFIG_RECHECK_BATCHES: u32 = 256;

/// What Redis sends before hanging up on connection number `maxclients + 1`.
const MAX_CLIENTS_ERROR: &[u8] = b"-ERR max number of clients reached\r\n";

/// Everything the loop needs that does not come from `ClientState`.
struct Conn {
    read_buf: BytesMut,
    out: OutputBuffer,
    parser: RequestParser,
    limits: OutputLimits,
    entry: Arc<ConnEntry>,
    batches: u32,
    /// Read-buffer size as of the last `read()`, for `CLIENT LIST`'s `rbs`.
    /// Sampled there rather than at publish time because `split_to` narrows
    /// the window as the batch is consumed, and `rbs` means the size of the
    /// buffer, not what is left of it.
    read_size: usize,
    /// Peak of `read_size`, for `CLIENT LIST`'s `rbp`.
    read_peak: usize,
    tot_net_in: u64,
    tot_net_out: u64,
}

/// Serve one TCP connection.
pub async fn serve_connection(
    server: Arc<ServerShared>,
    stream: TcpStream,
    peer: SocketAddr,
) -> io::Result<()> {
    let laddr = stream
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let fd = raw_fd(&stream);
    serve_stream(server, stream, peer.to_string(), laddr, fd, false).await
}

/// Serve one connection over any byte stream. Split out so the unix-socket
/// listener shares every line of the protocol path with TCP.
pub async fn serve_stream<S>(
    server: Arc<ServerShared>,
    mut stream: S,
    addr: String,
    laddr: String,
    fd: i32,
    unix: bool,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // ---- admission control (`maxclients`) ---------------------------------
    let cfg = server.config();
    let maxclients = cfg.maxclients;
    drop(cfg);
    if server.clients.len() >= maxclients {
        Stats::bump(&server.stats.rejected_connections);
        let _ = stream.write_all(MAX_CLIENTS_ERROR).await;
        let _ = stream.flush().await;
        return Ok(());
    }

    let now = server.clock.now_ms();
    let requires_auth = server.config().requirepass.is_some();
    let id = server.next_client_id();
    let (tx, mut oob_rx) = tokio::sync::mpsc::unbounded_channel::<OutOfBand>();

    let mut client = ClientState::new(id, addr.clone(), laddr.clone(), now, requires_auth);
    client.fd = fd;
    if unix {
        client.flags |= ClientFlags::UNIX_SOCKET;
    }
    client.oob = Some(tx.clone());

    // The frozen registry, which pub/sub delivery (W3b) uses...
    let handle = Arc::new(ClientHandle {
        id,
        addr,
        laddr,
        name: parking_lot::Mutex::new(bytes::Bytes::new()),
        tx: tx.clone(),
        created_ms: now,
    });
    server.clients.insert(Arc::clone(&handle));

    // ...and W1b's, which carries the rest of what `CLIENT LIST` reports.
    let entry = Arc::new(ConnEntry::new(ConnSnapshot::new(&client, fd), tx));
    registry::register(&server, Arc::clone(&entry));

    let mut conn = Conn {
        read_buf: BytesMut::with_capacity(READ_BUF),
        out: OutputBuffer::new(READ_BUF, BUF_SHRINK_THRESHOLD),
        parser: RequestParser::new(server.config().proto_max_bulk_len),
        limits: OutputLimits::default(),
        entry,
        batches: 0,
        read_size: READ_BUF,
        read_peak: READ_BUF,
        tot_net_in: 0,
        tot_net_out: 0,
    };

    let result = connection_loop(&server, &mut client, &mut conn, &mut stream, &mut oob_rx).await;

    registry::unregister(&server, id);
    server.clients.remove(id);
    result
}

async fn connection_loop<S>(
    server: &Arc<ServerShared>,
    client: &mut ClientState,
    conn: &mut Conn,
    stream: &mut S,
    oob_rx: &mut tokio::sync::mpsc::UnboundedReceiver<OutOfBand>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        if server.shutting_down.load(Ordering::Relaxed) {
            return Ok(());
        }

        // `timeout 0` disables the idle check; `pending()` then costs nothing
        // and never wakes the task.
        let idle_timeout_ms = server.config().timeout.saturating_mul(1000);

        // Wait for either client input, an out-of-band frame, or the idle
        // deadline.
        let read_n = tokio::select! {
            biased;
            msg = oob_rx.recv() => {
                match msg {
                    Some(OutOfBand::Frame(f)) => {
                        // The zero-copy write path: the frame is queued for
                        // `writev`, never memcpy'd into the staging buffer.
                        conn.out.push_bytes(f);
                        if let Some(breach) = check_output_limit(server, client, conn) {
                            debug!(id = client.id, ?breach, "output buffer limit reached");
                            conn.out.discard();
                            return Ok(());
                        }
                        flush(stream, conn, server).await?;
                        continue;
                    }
                    // Owner: W3b. Nothing can block yet, so these are no-ops.
                    Some(OutOfBand::Unblock { .. }) => continue,
                    Some(OutOfBand::Kill) | None => {
                        // Deliver whatever is already staged, then hang up.
                        let _ = flush(stream, conn, server).await;
                        return Ok(());
                    }
                }
            }
            n = stream.read_buf(&mut conn.read_buf) => n?,
            _ = idle(idle_timeout_ms) => {
                debug!(id = client.id, timeout_ms = idle_timeout_ms, "closing idle connection");
                return Ok(());
            }
        };

        if read_n == 0 {
            // Clean EOF.
            return Ok(());
        }
        Stats::add(&server.stats.net_input_bytes, read_n as u64);
        Stats::bump(&server.stats.total_reads_processed);
        conn.tot_net_in += read_n as u64;
        conn.read_size = conn.read_buf.capacity();
        if conn.read_size > conn.read_peak {
            conn.read_peak = conn.read_size;
        }

        // §5.6: re-anchor the coarse clock once per batch, not per command.
        // The 1 ms ticker normally does this; doing it here too means expiry
        // stays correct even if that task is starved.
        let _ = server.clock.refresh();
        client.last_interaction_ms = server.clock.now_ms();

        // Pick up a changed `proto-max-bulk-len` occasionally without paying
        // for a config load on every command. Updating the limit in place --
        // rather than rebuilding the parser -- keeps a half-read frame intact.
        conn.batches = conn.batches.wrapping_add(1);
        if conn.batches.is_multiple_of(CONFIG_RECHECK_BATCHES) {
            conn.parser
                .set_proto_max_bulk_len(server.config().proto_max_bulk_len);
        }

        // Publish *before* running the batch as well as after, so that a
        // `CLIENT INFO` inside this very batch sees this connection's real
        // `qbuf`, `rbs` and `tot-net-in` rather than the previous batch's --
        // and, on the first command of a connection, sees something other
        // than zeroes.
        publish(client, conn);

        let close = execute_batch(server, client, conn);

        // ---- one flush per batch ------------------------------------------
        flush(stream, conn, server).await?;
        publish(client, conn);

        if close || client.should_close() {
            return Ok(());
        }

        reclaim_buffers(conn);
    }
}

/// Run every complete command the read buffer holds. Returns true when the
/// connection must close once the reply is out.
fn execute_batch(server: &Arc<ServerShared>, client: &mut ClientState, conn: &mut Conn) -> bool {
    loop {
        let parsed = conn.parser.parse(&mut conn.read_buf);
        match parsed {
            Ok(Parsed::Incomplete) => return false,
            Ok(Parsed::Empty) => continue,
            Ok(Parsed::Command(args)) => {
                trace!(argc = args.len(), "dispatch");
                // Where this command's reply starts, so `CLIENT REPLY
                // OFF|SKIP` can drop exactly one reply and nothing else.
                let mark = conn.out.staging().len();
                match engine::dispatch(server, client, conn.out.staging(), &args) {
                    Outcome::Done => {}
                    Outcome::Close => {
                        engine::post_command(client);
                        return true;
                    }
                    Outcome::Blocked(req) => {
                        // Owner: W3b. Until the blocking machinery exists,
                        // reply with the timeout value straight away rather
                        // than silently dropping the command -- that is the
                        // honest degradation, and it keeps the protocol in
                        // sync.
                        let mut w = ReplyWriter::new(conn.out.staging(), client.proto);
                        if req.kind.timeout_is_null_array() {
                            w.null_array();
                        } else {
                            w.int(0);
                        }
                    }
                }
                if client.reply_suppressed() {
                    conn.out.staging().truncate(mark);
                }
                engine::post_command(client);
            }
            Err(e) => {
                // A protocol error is unrecoverable: the stream can no longer
                // be resynchronised, so reply and hang up.
                let mut w = ReplyWriter::new(conn.out.staging(), client.proto);
                w.error_str(&e.wire_message());
                Stats::bump(&server.stats.total_error_replies);
                return true;
            }
        }
    }
}

/// Push this connection's current state to the registry, for `CLIENT LIST`.
/// Twice per batch -- before executing and after flushing -- which is one
/// uncontended lock next to each syscall.
fn publish(client: &ClientState, conn: &mut Conn) {
    let qbuf = conn.read_buf.len();
    let qbuf_free = conn.read_buf.capacity().saturating_sub(qbuf);
    let rbs = conn.read_size;
    let (rbp, oll, omem, tot_mem) = (
        conn.read_peak,
        conn.out.queued_frames(),
        conn.out.peak_pending(),
        conn.out.memory_usage() + rbs,
    );
    let obl = conn.out.pending();
    let (tot_net_in, tot_net_out) = (conn.tot_net_in, conn.tot_net_out);
    conn.entry.update(|s| {
        s.refresh_from(client);
        s.qbuf = qbuf;
        s.qbuf_free = qbuf_free;
        s.rbs = rbs;
        s.rbp = rbp;
        s.obl = obl;
        s.oll = oll;
        s.omem = omem;
        s.tot_mem = tot_mem;
        s.tot_net_in = tot_net_in;
        s.tot_net_out = tot_net_out;
    });
}

/// Which `client-output-buffer-limit` class this connection is in, and whether
/// it has blown its budget.
fn check_output_limit(
    server: &Arc<ServerShared>,
    client: &ClientState,
    conn: &mut Conn,
) -> Option<LimitBreach> {
    let class = if client.subs.in_subscribe_mode() {
        ClientClass::PubSub
    } else {
        ClientClass::Normal
    };
    let limit = conn.limits.for_class(class);
    conn.out.check_limit(limit, server.clock.now_ms())
}

/// Give memory back once a burst is over.
fn reclaim_buffers(conn: &mut Conn) {
    if conn.read_buf.is_empty()
        && !conn.parser.is_mid_frame()
        && conn.read_buf.capacity() > BUF_SHRINK_THRESHOLD
    {
        conn.read_buf = BytesMut::with_capacity(READ_BUF);
    }
}

async fn flush<S>(stream: &mut S, conn: &mut Conn, server: &Arc<ServerShared>) -> io::Result<()>
where
    S: AsyncWrite + Unpin,
{
    if conn.out.is_empty() {
        return Ok(());
    }
    let n = conn.out.flush(stream).await?;
    Stats::add(&server.stats.net_output_bytes, n);
    Stats::bump(&server.stats.total_writes_processed);
    conn.tot_net_out += n;
    Ok(())
}

/// The idle-timeout arm of the select. `timeout 0` means "never".
async fn idle(ms: u64) {
    if ms == 0 {
        std::future::pending::<()>().await
    } else {
        tokio::time::sleep(Duration::from_millis(ms)).await
    }
}

#[cfg(unix)]
fn raw_fd<T: std::os::fd::AsRawFd>(s: &T) -> i32 {
    s.as_raw_fd()
}

#[cfg(not(unix))]
fn raw_fd<T>(_s: &T) -> i32 {
    -1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[tokio::test]
    async fn maxclients_rejects_with_the_redis_error() {
        // A server that admits nobody: the very first connection is refused
        // with Redis's exact string, and the counter moves.
        let server = ServerShared::new(Config {
            shard_count: 2,
            maxclients: 0,
            ..Default::default()
        });
        let (a, b) = tokio::io::duplex(1024);
        let srv = Arc::clone(&server);
        let task = tokio::spawn(async move {
            serve_stream(srv, a, "c:1".into(), "s:1".into(), -1, false).await
        });
        let mut b = b;
        let mut got = Vec::new();
        b.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, MAX_CLIENTS_ERROR);
        task.await.unwrap().unwrap();
        assert_eq!(Stats::get(&server.stats.rejected_connections), 1);
    }

    #[tokio::test]
    async fn idle_timeout_closes_the_connection() {
        let server = ServerShared::new(Config {
            shard_count: 2,
            // `timeout` is in seconds; 1 is the smallest value Redis accepts.
            timeout: 1,
            ..Default::default()
        });
        let (a, b) = tokio::io::duplex(1024);
        let srv = Arc::clone(&server);
        tokio::spawn(
            async move { serve_stream(srv, a, "c:1".into(), "s:1".into(), -1, false).await },
        );
        let mut b = b;
        let mut got = Vec::new();
        // No traffic at all: the server must hang up on its own.
        tokio::time::timeout(Duration::from_secs(10), b.read_to_end(&mut got))
            .await
            .expect("connection was never closed")
            .unwrap();
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn a_batch_is_answered_with_one_flush() {
        let server = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let (a, mut b) = tokio::io::duplex(64 * 1024);
        let srv = Arc::clone(&server);
        tokio::spawn(
            async move { serve_stream(srv, a, "c:1".into(), "s:1".into(), -1, false).await },
        );
        b.write_all(b"*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n")
            .await
            .unwrap();
        let mut got = vec![0u8; 64];
        let n = b.read(&mut got).await.unwrap();
        assert_eq!(&got[..n], b"+PONG\r\n+PONG\r\n+PONG\r\n");
        assert_eq!(Stats::get(&server.stats.total_writes_processed), 1);
    }

    #[tokio::test]
    async fn client_reply_off_and_on() {
        let server = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let (a, mut b) = tokio::io::duplex(64 * 1024);
        let srv = Arc::clone(&server);
        tokio::spawn(
            async move { serve_stream(srv, a, "c:1".into(), "s:1".into(), -1, false).await },
        );
        b.write_all(b"CLIENT REPLY OFF\r\nPING\r\nPING\r\nCLIENT REPLY ON\r\nPING\r\n")
            .await
            .unwrap();
        let mut got = vec![0u8; 128];
        let n = b.read(&mut got).await.unwrap();
        assert_eq!(
            &got[..n],
            b"+OK\r\n+PONG\r\n",
            "only CLIENT REPLY ON and what follows it may reply"
        );
    }

    #[tokio::test]
    async fn client_reply_skip_drops_exactly_one_reply() {
        let server = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let (a, mut b) = tokio::io::duplex(64 * 1024);
        let srv = Arc::clone(&server);
        tokio::spawn(
            async move { serve_stream(srv, a, "c:1".into(), "s:1".into(), -1, false).await },
        );
        b.write_all(b"CLIENT REPLY SKIP\r\nECHO one\r\nECHO two\r\n")
            .await
            .unwrap();
        let mut got = vec![0u8; 128];
        let n = b.read(&mut got).await.unwrap();
        assert_eq!(&got[..n], b"$3\r\ntwo\r\n");
    }

    #[tokio::test]
    async fn the_read_buffer_shrinks_back_after_a_large_value() {
        let server = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let (a, mut b) = tokio::io::duplex(1 << 20);
        let srv = Arc::clone(&server);
        tokio::spawn(
            async move { serve_stream(srv, a, "c:1".into(), "s:1".into(), -1, false).await },
        );

        let big = vec![b'v'; 1 << 20];
        let mut req = Vec::from(&b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n"[..]);
        req.extend_from_slice(format!("${}\r\n", big.len()).as_bytes());
        req.extend_from_slice(&big);
        req.extend_from_slice(b"\r\n");
        b.write_all(&req).await.unwrap();
        let mut got = vec![0u8; 16];
        let n = b.read(&mut got).await.unwrap();
        assert_eq!(&got[..n], b"+OK\r\n");

        // The next command proves the connection still works after the
        // buffers were reclaimed.
        b.write_all(b"PING\r\n").await.unwrap();
        let n = b.read(&mut got).await.unwrap();
        assert_eq!(&got[..n], b"+PONG\r\n");

        let entry = registry::snapshot(&server);
        let snap = entry.first().expect("one live connection").snapshot();
        assert!(
            snap.rbs <= BUF_SHRINK_THRESHOLD,
            "read buffer stayed at {} bytes",
            snap.rbs
        );
        assert!(snap.rbp > BUF_SHRINK_THRESHOLD, "peak was not recorded");
    }

    #[tokio::test]
    async fn a_protocol_error_replies_then_closes() {
        let server = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let (a, mut b) = tokio::io::duplex(1024);
        let srv = Arc::clone(&server);
        tokio::spawn(
            async move { serve_stream(srv, a, "c:1".into(), "s:1".into(), -1, false).await },
        );
        b.write_all(b"*1\r\nxbad\r\n").await.unwrap();
        let mut got = Vec::new();
        b.read_to_end(&mut got).await.unwrap();
        assert_eq!(got, b"-ERR Protocol error: expected '$', got 'x'\r\n");
    }

    #[tokio::test]
    async fn an_out_of_band_frame_is_delivered_without_a_command() {
        let server = ServerShared::new(Config {
            shard_count: 2,
            ..Default::default()
        });
        let (a, mut b) = tokio::io::duplex(1024);
        let srv = Arc::clone(&server);
        tokio::spawn(
            async move { serve_stream(srv, a, "c:1".into(), "s:1".into(), -1, false).await },
        );
        // Wait for registration, then push a frame in from outside.
        let handle = loop {
            if let Some(h) = server.clients.get(1) {
                break h;
            }
            tokio::task::yield_now().await;
        };
        handle
            .tx
            .send(OutOfBand::Frame(bytes::Bytes::from_static(b"+HI\r\n")))
            .unwrap();
        let mut got = vec![0u8; 16];
        let n = b.read(&mut got).await.unwrap();
        assert_eq!(&got[..n], b"+HI\r\n");
    }
}
