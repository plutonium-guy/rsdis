//! Protocol and connection-layer benchmarks (W1b).
//!
//! # How to run
//!
//! ```text
//! cargo test --release --bench protocol_bench -- --ignored --nocapture
//! ```
//!
//! **Not criterion, and here is why.** `Cargo.toml` declares no `[[bench]]`
//! target, so cargo auto-discovers this file with the default libtest harness
//! (`harness = true`). Criterion requires `harness = false`, which can only be
//! set in `Cargo.toml` -- an F0-owned file W1b must not edit. Rather than
//! silently produce a bench target that compiles and measures nothing, these
//! are `#[ignore]`d tests that time themselves and print a table. Reported as
//! a contract gap: `Cargo.toml` needs
//!
//! ```toml
//! [[bench]]
//! name = "protocol_bench"
//! harness = false
//! ```
//!
//! before criterion can be used here (or in any other agent's bench file).
//!
//! Every measurement below is a steady-state loop after a warm-up, reported as
//! throughput. Numbers are only meaningful from a `--release` build; the
//! harness says so if it detects otherwise.

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use rsdis::net::OutputBuffer;
use rsdis::reply::{RESP2, ReplyWriter};
use rsdis::resp::parser::{Parsed, RequestParser};

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

/// Run `f` until at least `min_time` has elapsed, and report per-iteration
/// cost. `f` returns the number of logical units it processed.
fn measure(label: &str, unit: &str, min_time: Duration, mut f: impl FnMut() -> u64) {
    // Warm up: fault in pages, settle the branch predictors.
    for _ in 0..3 {
        black_box(f());
    }
    let start = Instant::now();
    let mut units = 0u64;
    let mut iters = 0u64;
    while start.elapsed() < min_time {
        units += f();
        iters += 1;
    }
    let elapsed = start.elapsed();
    let per_unit_ns = elapsed.as_nanos() as f64 / units as f64;
    let rate = units as f64 / elapsed.as_secs_f64();
    println!("{label:<46} {rate:>14.0} {unit}/s   {per_unit_ns:>9.1} ns/{unit}  ({iters} iters)");
}

/// Same, but the unit is bytes, so report bandwidth too.
fn measure_bytes(label: &str, min_time: Duration, mut f: impl FnMut() -> u64) {
    for _ in 0..3 {
        black_box(f());
    }
    let start = Instant::now();
    let mut bytes = 0u64;
    while start.elapsed() < min_time {
        bytes += f();
    }
    let elapsed = start.elapsed();
    let gbps = bytes as f64 / elapsed.as_secs_f64() / 1e9;
    println!("{label:<46} {gbps:>14.2} GB/s");
}

fn note_profile() {
    if cfg!(debug_assertions) {
        println!(
            "\n  !! debug build: these numbers are meaningless. \
             Re-run with `cargo test --release --bench protocol_bench -- --ignored --nocapture`.\n"
        );
    }
}

const RUN: Duration = Duration::from_millis(500);

// ---------------------------------------------------------------------------
// request parsing
// ---------------------------------------------------------------------------

/// `n` pipelined copies of a small command, as one contiguous buffer.
fn pipeline(n: usize, argv: &[&[u8]]) -> Vec<u8> {
    let mut wire = Vec::new();
    for _ in 0..n {
        wire.extend_from_slice(format!("*{}\r\n", argv.len()).as_bytes());
        for a in argv {
            wire.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
            wire.extend_from_slice(a);
            wire.extend_from_slice(b"\r\n");
        }
    }
    wire
}

fn drain(parser: &mut RequestParser, buf: &mut BytesMut) -> u64 {
    let mut n = 0u64;
    loop {
        match parser.parse(buf) {
            Ok(Parsed::Command(args)) => {
                black_box(&args);
                n += 1;
            }
            Ok(Parsed::Empty) => {}
            Ok(Parsed::Incomplete) => return n,
            Err(e) => panic!("benchmark input must parse: {e}"),
        }
    }
}

#[test]
#[ignore = "benchmark"]
fn bench_parse() {
    note_profile();
    println!("\n=== request parsing ===");

    // The three shapes that dominate a real workload.
    let cases: &[(&str, &[&[u8]])] = &[
        ("GET foo", &[b"GET", b"foo"]),
        ("SET foo bar", &[b"SET", b"foo", b"bar"]),
        (
            "HSET h f1 v1 f2 v2",
            &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"],
        ),
    ];

    for (name, argv) in cases {
        let wire = pipeline(1000, argv);
        let label = format!("multibulk pipeline x1000: {name}");
        measure(&label, "cmd", RUN, || {
            let mut buf = BytesMut::from(&wire[..]);
            let mut parser = RequestParser::default();
            drain(&mut parser, &mut buf)
        });
    }

    // Inline commands take the `sdssplitargs` path instead.
    let inline: Vec<u8> = std::iter::repeat_n("GET foo\r\n", 1000)
        .collect::<String>()
        .into();
    measure("inline pipeline x1000: GET foo", "cmd", RUN, || {
        let mut buf = BytesMut::from(&inline[..]);
        let mut parser = RequestParser::default();
        drain(&mut parser, &mut buf)
    });

    // Bandwidth over the same input, so the number is comparable to memcpy.
    let wire = pipeline(1000, &[b"SET", b"foo", b"bar"]);
    let len = wire.len() as u64;
    measure_bytes("multibulk parse bandwidth", RUN, || {
        let mut buf = BytesMut::from(&wire[..]);
        let mut parser = RequestParser::default();
        black_box(drain(&mut parser, &mut buf));
        len
    });
}

/// The incrementality claim, measured rather than asserted: a 1 MB bulk fed in
/// small chunks. A parser that restarts the scan on every read is quadratic in
/// the number of chunks; this one is linear.
#[test]
#[ignore = "benchmark"]
fn bench_parse_incremental() {
    note_profile();
    println!("\n=== incremental parsing: 1 MiB bulk, by chunk size ===");
    println!(
        "(a restart-from-scratch parser is quadratic here; linear means the state machine works)"
    );

    let size = 1 << 20;
    let value = vec![b'v'; size];
    let mut frame = Vec::from(&b"*2\r\n$3\r\nSET\r\n"[..]);
    frame.extend_from_slice(format!("${size}\r\n").as_bytes());
    frame.extend_from_slice(&value);
    frame.extend_from_slice(b"\r\n");

    for chunk in [64usize, 512, 4096, 65536] {
        let label = format!("1 MiB bulk in {chunk} B chunks");
        measure(&label, "MiB", RUN, || {
            let mut parser = RequestParser::default();
            let mut buf = BytesMut::with_capacity(frame.len());
            let mut off = 0;
            while off < frame.len() {
                let end = (off + chunk).min(frame.len());
                buf.extend_from_slice(frame.get(off..end).unwrap_or_default());
                off = end;
                black_box(drain(&mut parser, &mut buf));
            }
            1
        });
    }
}

// ---------------------------------------------------------------------------
// reply encoding
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark"]
fn bench_reply_encoding() {
    note_profile();
    println!("\n=== reply encoding into the connection buffer ===");

    for size in [8usize, 512, 64 * 1024] {
        let value = Bytes::from(vec![b'x'; size]);
        let label = format!("bulk_from, {size} B value");
        measure(&label, "reply", RUN, || {
            let mut buf = BytesMut::with_capacity(size + 64);
            let mut w = ReplyWriter::new(&mut buf, RESP2);
            for _ in 0..64 {
                buf_clear(&mut w);
                w.bulk_from(&value);
            }
            64
        });

        let label = format!("bulk_from bandwidth, {size} B value");
        let bytes = size as u64 * 64;
        measure_bytes(&label, RUN, || {
            let mut buf = BytesMut::with_capacity(size + 64);
            let mut w = ReplyWriter::new(&mut buf, RESP2);
            for _ in 0..64 {
                buf_clear(&mut w);
                w.bulk_from(&value);
            }
            bytes
        });
    }

    // A batch of small replies, which is what a pipelined GET workload looks
    // like on the way out.
    let value = Bytes::from_static(b"hello world");
    measure(
        "1000 small bulk replies into one buffer",
        "reply",
        RUN,
        || {
            let mut buf = BytesMut::with_capacity(16 * 1024);
            let mut w = ReplyWriter::new(&mut buf, RESP2);
            for _ in 0..1000 {
                w.bulk_from(&value);
            }
            1000
        },
    );
}

/// `ReplyWriter` has no `clear`, and `raw()` is the sanctioned escape hatch.
fn buf_clear(w: &mut ReplyWriter<'_>) {
    w.raw().clear();
}

// ---------------------------------------------------------------------------
// §9.10: what vectored writes would actually buy
// ---------------------------------------------------------------------------

/// The measurement behind the §9.10 decision.
///
/// Two ways to get `n` values of `size` bytes onto a socket:
///
/// * **memcpy** -- what `ReplyWriter::bulk_from` does today: copy each value
///   into the connection's `BytesMut`, then one `write`.
/// * **writev** -- what §9.10 proposes: queue each value's `Bytes` and hand
///   the kernel an iovec array, copying nothing in user space.
///
/// Both go over a real loopback TCP socket with a real reader draining it, so
/// the syscall and the kernel-side copy are both in the measurement.
#[test]
#[ignore = "benchmark"]
fn bench_writev_vs_memcpy() {
    note_profile();
    println!("\n=== §9.10: memcpy-then-write vs writev, over loopback TCP ===");
    println!("(headers are 32 B each; 'n' values per flush)");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");

    for (n, size) in [
        (16usize, 8usize),
        (16, 512),
        (16, 2 * 1024),
        (16, 4 * 1024),
        (16, 8 * 1024),
        (16, 64 * 1024),
        (128, 512),
        (128, 64 * 1024),
    ] {
        rt.block_on(async move {
            let values: Vec<Bytes> = (0..n).map(|_| Bytes::from(vec![b'x'; size])).collect();
            let total = (n * size) as u64;

            let (mut writer, reader) = pair().await;
            let sink = tokio::spawn(drain_socket(reader));

            // --- memcpy path -------------------------------------------------
            let mut out = OutputBuffer::new(16 * 1024, 1 << 20);
            let start = Instant::now();
            let mut rounds = 0u64;
            while start.elapsed() < Duration::from_millis(250) {
                for v in &values {
                    out.staging().extend_from_slice(v);
                }
                out.flush(&mut writer).await.expect("write");
                rounds += 1;
            }
            let memcpy = start.elapsed().as_secs_f64() / rounds as f64;

            // --- writev path -------------------------------------------------
            let start = Instant::now();
            let mut rounds = 0u64;
            while start.elapsed() < Duration::from_millis(250) {
                for v in &values {
                    out.push_bytes(v.clone());
                }
                out.flush(&mut writer).await.expect("write");
                rounds += 1;
            }
            let vectored = start.elapsed().as_secs_f64() / rounds as f64;

            drop(writer);
            let _ = sink.await;

            let delta = (memcpy - vectored) / memcpy * 100.0;
            println!(
                "n={n:<4} size={size:<7} memcpy {:>9.2} us/flush   writev {:>9.2} us/flush   \
                 {delta:>+7.1}%  ({:.2} GB/s vs {:.2} GB/s)",
                memcpy * 1e6,
                vectored * 1e6,
                total as f64 / memcpy / 1e9,
                total as f64 / vectored / 1e9,
            );
        });
    }

    println!(
        "\nReading: a positive percentage means writev won. The crossover is what\n\
         `net::output::VECTORED_MIN_BYTES` encodes."
    );
}

async fn pair() -> (tokio::net::TcpStream, tokio::net::TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let connect = tokio::net::TcpStream::connect(addr);
    let (client, (server, _)) = tokio::join!(connect, async { listener.accept().await.unwrap() });
    let client = client.expect("connect");
    client.set_nodelay(true).expect("nodelay");
    server.set_nodelay(true).expect("nodelay");
    (client, server)
}

async fn drain_socket(mut s: tokio::net::TcpStream) {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        match s.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

// ---------------------------------------------------------------------------
// end to end
// ---------------------------------------------------------------------------

/// The number that matters most: commands per second through the real server,
/// over a real socket, with a real pipeline. This is the rsdis analogue of
/// `redis-benchmark -P 100`.
#[test]
#[ignore = "benchmark"]
fn bench_end_to_end_pipeline() {
    note_profile();
    println!("\n=== end to end: pipelined commands through the server ===");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");

    rt.block_on(async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let server = rsdis::ctx::ServerShared::new(rsdis::config::Config {
            port: 0,
            bind: vec!["127.0.0.1".into()],
            shard_count: 8,
            ..Default::default()
        });
        let _ticker = server.spawn_clock_ticker();
        let handle = rsdis::net::serve(std::sync::Arc::clone(&server))
            .await
            .expect("bind");
        let addr = handle.local_addr().expect("addr");

        for depth in [1usize, 16, 128] {
            let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
            s.set_nodelay(true).expect("nodelay");

            let request = pipeline(depth, &[b"SET", b"bench:key", b"value"]);
            // `+OK\r\n` per command.
            let expect = depth * 5;
            let mut buf = vec![0u8; expect.max(4096)];

            // Warm up.
            for _ in 0..10 {
                s.write_all(&request).await.expect("write");
                read_exactly(&mut s, &mut buf, expect).await;
            }

            let start = Instant::now();
            let mut commands = 0u64;
            while start.elapsed() < RUN {
                s.write_all(&request).await.expect("write");
                read_exactly(&mut s, &mut buf, expect).await;
                commands += depth as u64;
            }
            let rate = commands as f64 / start.elapsed().as_secs_f64();
            println!("SET, pipeline depth {depth:<4}          {rate:>14.0} cmd/s");
        }

        // GET of a 64 KiB value, the case §9.10 is about.
        for size in [8usize, 512, 64 * 1024] {
            let mut s = tokio::net::TcpStream::connect(addr).await.expect("connect");
            s.set_nodelay(true).expect("nodelay");
            let value = vec![b'x'; size];
            let mut set = Vec::from(&b"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n"[..]);
            set.extend_from_slice(format!("${size}\r\n").as_bytes());
            set.extend_from_slice(&value);
            set.extend_from_slice(b"\r\n");
            s.write_all(&set).await.expect("write");
            let mut ok = [0u8; 5];
            s.read_exact(&mut ok).await.expect("read");

            let depth = 16;
            let get = pipeline(depth, &[b"GET", b"big"]);
            let expect = depth * (size + 2 + format!("${size}\r\n").len());
            let mut buf = vec![0u8; expect];

            for _ in 0..5 {
                s.write_all(&get).await.expect("write");
                read_exactly(&mut s, &mut buf, expect).await;
            }
            let start = Instant::now();
            let mut bytes = 0u64;
            let mut commands = 0u64;
            while start.elapsed() < RUN {
                s.write_all(&get).await.expect("write");
                read_exactly(&mut s, &mut buf, expect).await;
                bytes += (depth * size) as u64;
                commands += depth as u64;
            }
            let secs = start.elapsed().as_secs_f64();
            println!(
                "GET, {size:>6} B value, depth 16    {:>14.0} cmd/s   {:.2} GB/s",
                commands as f64 / secs,
                bytes as f64 / secs / 1e9
            );
        }

        handle.shutdown();
    });
}

async fn read_exactly(s: &mut tokio::net::TcpStream, buf: &mut [u8], n: usize) {
    use tokio::io::AsyncReadExt;
    let mut got = 0usize;
    while got < n {
        let slice = buf.get_mut(got..n).expect("buffer is large enough");
        let r = s.read(slice).await.expect("read");
        assert!(r > 0, "server closed mid-reply");
        got += r;
    }
}
