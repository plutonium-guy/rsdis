//! The per-connection read/execute/flush loop.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! # Pipelining (§2.2)
//!
//! One iteration = one `read()`, then *every* complete command in the buffer
//! is executed into a single output buffer, then *one* `write_all()`. A client
//! that pipelines 1000 `SET`s pays one syscall each way, not 1000.
//!
//! # Locks and `.await`
//!
//! `engine::dispatch` is synchronous and releases every shard guard before it
//! returns, so no `parking_lot` guard is ever held across a suspension point.
//! That is load-bearing for the deadlock argument in `engine.rs`; keep it that
//! way.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::trace;

use crate::ctx::{ClientHandle, ClientState, OutOfBand, ServerShared};
use crate::engine::{self, Outcome};
use crate::info::Stats;
use crate::reply::ReplyWriter;
use crate::resp::parser::{Parsed, RequestParser};

/// Initial read buffer. Grows on demand; 16 KiB covers a typical pipelined
/// burst without a realloc.
const READ_BUF: usize = 16 * 1024;
/// Shrink the buffers back down once they exceed this and are empty, so one
/// large request does not pin memory for the connection's lifetime.
const BUF_SHRINK_THRESHOLD: usize = 256 * 1024;

pub async fn serve_connection(
    server: Arc<ServerShared>,
    mut stream: TcpStream,
    peer: SocketAddr,
) -> io::Result<()> {
    let laddr = stream
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let now = server.clock.now_ms();
    let requires_auth = server.config().requirepass.is_some();

    let id = server.next_client_id();
    let (tx, mut oob_rx) = tokio::sync::mpsc::unbounded_channel::<OutOfBand>();

    let mut client = ClientState::new(id, peer.to_string(), laddr.clone(), now, requires_auth);
    client.oob = Some(tx.clone());

    let handle = Arc::new(ClientHandle {
        id,
        addr: peer.to_string(),
        laddr,
        name: parking_lot::Mutex::new(bytes::Bytes::new()),
        tx,
        created_ms: now,
    });
    server.clients.insert(Arc::clone(&handle));

    let result = connection_loop(&server, &mut client, &mut stream, &mut oob_rx).await;
    server.clients.remove(id);
    result
}

async fn connection_loop(
    server: &Arc<ServerShared>,
    client: &mut ClientState,
    stream: &mut TcpStream,
    oob_rx: &mut tokio::sync::mpsc::UnboundedReceiver<OutOfBand>,
) -> io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(READ_BUF);
    let mut out = BytesMut::with_capacity(READ_BUF);
    let mut parser = RequestParser::new(server.config().proto_max_bulk_len);
    let mut config_generation_check = 0u32;

    loop {
        // Wait for either client input or an out-of-band frame.
        let read_n = tokio::select! {
            biased;
            msg = oob_rx.recv() => {
                match msg {
                    Some(OutOfBand::Frame(f)) => {
                        out.extend_from_slice(&f);
                        flush(stream, &mut out, server).await?;
                        continue;
                    }
                    // Owner: W3b. Nothing can block yet, so these are no-ops.
                    Some(OutOfBand::Unblock { .. }) => continue,
                    Some(OutOfBand::Kill) | None => return Ok(()),
                }
            }
            n = stream.read_buf(&mut read_buf) => n?,
        };

        if read_n == 0 {
            // Clean EOF.
            return Ok(());
        }
        Stats::add(&server.stats.net_input_bytes, read_n as u64);
        Stats::bump(&server.stats.total_reads_processed);

        // §5.6: re-anchor the coarse clock once per batch, not per command.
        // The 1 ms ticker normally does this; doing it here too means expiry
        // stays correct even if that task is starved.
        let _ = server.clock.refresh();
        client.last_interaction_ms = server.clock.now_ms();

        // Pick up a changed `proto-max-bulk-len` occasionally without paying
        // for a config load on every command.
        config_generation_check = config_generation_check.wrapping_add(1);
        if config_generation_check.is_multiple_of(256) {
            parser = RequestParser::new(server.config().proto_max_bulk_len);
        }

        let mut close = false;

        // ---- execute everything the buffer holds --------------------------
        loop {
            match parser.parse(&mut read_buf) {
                Ok(Parsed::Incomplete) => break,
                Ok(Parsed::Empty) => continue,
                Ok(Parsed::Command(args)) => {
                    trace!(argc = args.len(), "dispatch");
                    match engine::dispatch(server, client, &mut out, &args) {
                        Outcome::Done => {}
                        Outcome::Close => {
                            close = true;
                            break;
                        }
                        Outcome::Blocked(req) => {
                            // Owner: W3b. Until the blocking machinery exists,
                            // reply with the timeout value straight away
                            // rather than silently dropping the command --
                            // that is the honest degradation, and it keeps the
                            // protocol in sync.
                            let mut w = ReplyWriter::new(&mut out, client.proto);
                            if req.kind.timeout_is_null_array() {
                                w.null_array();
                            } else {
                                w.int(0);
                            }
                        }
                    }
                    engine::post_command(client);
                }
                Err(e) => {
                    // A protocol error is unrecoverable: reply and hang up.
                    let mut w = ReplyWriter::new(&mut out, client.proto);
                    w.error_str(&e.wire_message());
                    close = true;
                    break;
                }
            }
        }

        // ---- one flush per batch ------------------------------------------
        flush(stream, &mut out, server).await?;

        if close || client.should_close() {
            return Ok(());
        }

        // Keep a long-lived idle connection from pinning a big buffer.
        if read_buf.is_empty() && read_buf.capacity() > BUF_SHRINK_THRESHOLD {
            read_buf = BytesMut::with_capacity(READ_BUF);
        }
        if out.capacity() > BUF_SHRINK_THRESHOLD {
            out = BytesMut::with_capacity(READ_BUF);
        }
    }
}

async fn flush(
    stream: &mut TcpStream,
    out: &mut BytesMut,
    server: &Arc<ServerShared>,
) -> io::Result<()> {
    if out.is_empty() {
        return Ok(());
    }
    stream.write_all(out).await?;
    Stats::add(&server.stats.net_output_bytes, out.len() as u64);
    Stats::bump(&server.stats.total_writes_processed);
    out.clear();
    Ok(())
}
