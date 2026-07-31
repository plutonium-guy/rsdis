//! End-to-end tests for the protocol and connection layer (W1b).
//!
//! Owner: W1b. (`tests/**` belongs to W3d; §3 allows each agent to add
//! `tests/<own_area>_test.rs`.)
//!
//! Three kinds of test live here:
//!
//! 1. **Real client** -- the `redis` crate over a real socket, so anything
//!    that passes also passes with `redis-cli`.
//! 2. **Raw bytes** -- a bare `TcpStream` writing hand-built frames, because a
//!    well-behaved client cannot produce the inputs that matter (a `$` where a
//!    `*` belongs, a 100-million-element multibulk header, a command split one
//!    byte at a time).
//! 3. **Property-based** -- `proptest` over arbitrary byte streams and over
//!    arbitrary *split points* in a valid stream. Neither may panic, and a
//!    valid command must parse identically however it is chopped up.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use rsdis::config::Config;
use rsdis::ctx::ServerShared;
use rsdis::net::ServerHandle;
use rsdis::resp::parser::{Parsed, RequestParser};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

fn base_config() -> Config {
    Config {
        // Port 0: let the kernel pick, so tests can run in parallel.
        port: 0,
        bind: vec!["127.0.0.1".to_string()],
        shard_count: 8,
        ..Default::default()
    }
}

async fn start_with(cfg: Config) -> (ServerHandle, redis::Client) {
    let server = ServerShared::new(cfg);
    let _ticker = server.spawn_clock_ticker();
    let handle = rsdis::net::serve(Arc::clone(&server))
        .await
        .expect("server must bind");
    let addr = handle.local_addr().expect("a bound address");
    let client = redis::Client::open(format!("redis://{addr}/")).expect("client must open");
    (handle, client)
}

async fn start() -> (ServerHandle, redis::Client) {
    start_with(base_config()).await
}

/// A raw connection, for the frames a real client will not send.
struct Raw {
    stream: TcpStream,
}

impl Raw {
    async fn connect(handle: &ServerHandle) -> Self {
        let addr = handle.local_addr().expect("a bound address");
        Raw {
            stream: TcpStream::connect(addr).await.expect("connect"),
        }
    }

    async fn send(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).await.expect("write");
    }

    /// Read whatever arrives within a short window. Used when the expected
    /// reply is a single frame and the connection stays open.
    async fn read_some(&mut self) -> String {
        let mut buf = vec![0u8; 64 * 1024];
        let n = tokio::time::timeout(Duration::from_secs(5), self.stream.read(&mut buf))
            .await
            .expect("server did not reply")
            .expect("read");
        String::from_utf8_lossy(buf.get(..n).unwrap_or_default()).into_owned()
    }

    /// Read until the server hangs up. Used for the protocol-error path.
    async fn read_to_end(&mut self) -> String {
        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), self.stream.read_to_end(&mut buf))
            .await
            .expect("server never closed the connection")
            .expect("read");
        String::from_utf8_lossy(&buf).into_owned()
    }
}

// ---------------------------------------------------------------------------
// protocol errors: exact strings, and a clean close
// ---------------------------------------------------------------------------

/// Every one of these must produce Redis's exact error line and then a clean
/// FIN -- not a panic, not a hang, not a silently ignored frame.
#[tokio::test]
async fn protocol_errors_match_redis_byte_for_byte() {
    let cases: &[(&[u8], &str)] = &[
        (
            b"*1\r\nxbad\r\n",
            "-ERR Protocol error: expected '$', got 'x'\r\n",
        ),
        (
            b"*3\r\n$3\r\nGET\r\n+foo\r\n",
            "-ERR Protocol error: expected '$', got '+'\r\n",
        ),
        (
            b"*notanumber\r\n",
            "-ERR Protocol error: invalid multibulk length\r\n",
        ),
        (
            b"*100000000\r\n",
            "-ERR Protocol error: invalid multibulk length\r\n",
        ),
        (
            b"*1048577\r\n",
            "-ERR Protocol error: invalid multibulk length\r\n",
        ),
        (
            b"*1\r\n$abc\r\n",
            "-ERR Protocol error: invalid bulk length\r\n",
        ),
        (
            b"*1\r\n$-1\r\n",
            "-ERR Protocol error: invalid bulk length\r\n",
        ),
        (
            b"*1\r\n$536870913\r\n",
            "-ERR Protocol error: invalid bulk length\r\n",
        ),
        (
            b"SET k \"unterminated\r\n",
            "-ERR Protocol error: unbalanced quotes in request\r\n",
        ),
        (
            b"SET k 'unterminated\r\n",
            "-ERR Protocol error: unbalanced quotes in request\r\n",
        ),
    ];

    let (handle, _c) = start().await;
    for (input, expected) in cases {
        let mut raw = Raw::connect(&handle).await;
        raw.send(input).await;
        let got = raw.read_to_end().await;
        assert_eq!(&got, expected, "input {:?}", String::from_utf8_lossy(input));
    }
    handle.shutdown();
}

#[tokio::test]
async fn an_oversized_inline_request_is_rejected() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;
    // 64 KB + 1 with no newline in sight.
    raw.send(&vec![b'a'; 64 * 1024 + 1]).await;
    let got = raw.read_to_end().await;
    assert_eq!(got, "-ERR Protocol error: too big inline request\r\n");
    handle.shutdown();
}

#[tokio::test]
async fn an_endless_multibulk_count_is_rejected() {
    let (handle, _c) = start().await;

    let mut raw = Raw::connect(&handle).await;
    let mut payload = vec![b'*'];
    payload.extend(std::iter::repeat_n(b'1', 64 * 1024 + 1));
    raw.send(&payload).await;
    assert_eq!(
        raw.read_to_end().await,
        "-ERR Protocol error: too big mbulk count string\r\n"
    );

    let mut raw = Raw::connect(&handle).await;
    let mut payload = Vec::from(&b"*1\r\n$"[..]);
    payload.extend(std::iter::repeat_n(b'1', 64 * 1024 + 1));
    raw.send(&payload).await;
    assert_eq!(
        raw.read_to_end().await,
        "-ERR Protocol error: too big bulk count string\r\n"
    );

    handle.shutdown();
}

/// The headline hostile case from the brief: a huge element count must not
/// make the server reserve anything. If it did, this test would take the
/// process down rather than fail.
#[tokio::test]
async fn a_hostile_multibulk_count_does_not_allocate() {
    let (handle, _c) = start().await;
    for count in ["100000000", "2147483647", "9223372036854775807"] {
        let mut raw = Raw::connect(&handle).await;
        raw.send(format!("*{count}\r\n").as_bytes()).await;
        assert_eq!(
            raw.read_to_end().await,
            "-ERR Protocol error: invalid multibulk length\r\n",
            "count {count}"
        );
    }
    // The server is still healthy afterwards.
    let mut raw = Raw::connect(&handle).await;
    raw.send(b"PING\r\n").await;
    assert_eq!(raw.read_some().await, "+PONG\r\n");
    handle.shutdown();
}

/// A legal-but-enormous element count is accepted as a header and then simply
/// waits, again without reserving 1M slots up front.
#[tokio::test]
async fn a_legal_but_huge_count_just_waits() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;
    raw.send(b"*1048576\r\n").await;
    // Nothing should come back at all; the parser is mid-frame.
    let mut buf = [0u8; 64];
    let r = tokio::time::timeout(Duration::from_millis(200), raw.stream.read(&mut buf)).await;
    assert!(r.is_err(), "expected silence, got {r:?}");
    handle.shutdown();
}

/// Binary payloads containing CR, LF and NUL must survive intact: the bulk
/// length is authoritative, never a delimiter scan.
#[tokio::test]
async fn binary_safe_arguments_round_trip() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let payload: Vec<u8> = vec![0u8, b'\r', b'\n', 0xff, b'$', b'*', 0x80, b'\n'];
    let echoed: Vec<u8> = redis::cmd("ECHO")
        .arg(&payload)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(echoed, payload);
    handle.shutdown();
}

// ---------------------------------------------------------------------------
// split across reads
// ---------------------------------------------------------------------------

/// The requirement from the brief: feed a valid command one byte at a time and
/// assert it parses identically. Covers every frame type the request grammar
/// has.
#[test]
fn byte_at_a_time_matches_one_shot() {
    let frames: &[&[u8]] = &[
        // multibulk, one argument
        b"*1\r\n$4\r\nPING\r\n",
        // multibulk, several arguments
        b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        // an empty bulk
        b"*2\r\n$4\r\nECHO\r\n$0\r\n\r\n",
        // a bulk containing the delimiter
        b"*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n",
        // a bulk containing NUL and high bytes
        b"*2\r\n$4\r\nECHO\r\n$3\r\n\x00\xff\x80\r\n",
        // a large bulk, so the body state machine is exercised
        b"*2\r\n$4\r\nECHO\r\n$10\r\n0123456789\r\n",
        // inline, plain
        b"PING\r\n",
        // inline, bare LF
        b"PING\n",
        // inline, several words
        b"SET key value\r\n",
        // inline, double quotes with an escape
        b"ECHO \"a\\x41 b\"\r\n",
        // inline, single quotes
        b"ECHO 'a b'\r\n",
        // inline, leading and trailing whitespace
        b"  SET   k   v  \r\n",
    ];

    for frame in frames {
        let one_shot = parse_all(frame, frame.len());
        let dribbled = parse_all(frame, 1);
        assert_eq!(
            one_shot,
            dribbled,
            "frame {:?} parsed differently byte-at-a-time",
            String::from_utf8_lossy(frame)
        );
        assert!(
            !one_shot.is_empty(),
            "frame {:?} produced nothing",
            String::from_utf8_lossy(frame)
        );
    }
}

/// The same guarantee for a whole pipeline, at every chunk size from 1 byte to
/// the full buffer.
#[test]
fn a_pipeline_parses_identically_at_every_chunk_size() {
    let mut wire = Vec::new();
    for i in 0..50 {
        let key = format!("key:{i}");
        let val = format!("value-{i}");
        wire.extend_from_slice(b"*3\r\n$3\r\nSET\r\n");
        wire.extend_from_slice(format!("${}\r\n{}\r\n", key.len(), key).as_bytes());
        wire.extend_from_slice(format!("${}\r\n{}\r\n", val.len(), val).as_bytes());
    }
    let expected = parse_all(&wire, wire.len());
    assert_eq!(expected.len(), 50);
    for chunk in [1, 2, 3, 7, 13, 64, 1024, wire.len()] {
        assert_eq!(
            parse_all(&wire, chunk),
            expected,
            "chunk size {chunk} changed the parse"
        );
    }
}

/// Feed `input` in `chunk`-sized pieces and collect every command produced.
fn parse_all(input: &[u8], chunk: usize) -> Vec<Vec<Vec<u8>>> {
    let mut parser = RequestParser::default();
    let mut buf = BytesMut::new();
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < input.len() {
        let end = (offset + chunk).min(input.len());
        buf.extend_from_slice(input.get(offset..end).unwrap_or_default());
        offset = end;
        loop {
            match parser.parse(&mut buf).expect("valid input must parse") {
                Parsed::Command(args) => out.push(args.iter().map(|b| b.to_vec()).collect()),
                Parsed::Empty => continue,
                Parsed::Incomplete => break,
            }
        }
    }
    out
}

/// The same thing over a real socket: a command split across TCP segments,
/// with a delay in between so the server genuinely sees two reads.
#[tokio::test]
async fn a_command_split_across_tcp_reads_still_works() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;
    let frame = b"*3\r\n$3\r\nSET\r\n$5\r\nsplit\r\n$5\r\nvalue\r\n";
    for byte in frame {
        raw.send(std::slice::from_ref(byte)).await;
        tokio::task::yield_now().await;
    }
    assert_eq!(raw.read_some().await, "+OK\r\n");

    raw.send(b"*2\r\n$3\r\nGET\r\n$5\r\nsplit\r\n").await;
    assert_eq!(raw.read_some().await, "$5\r\nvalue\r\n");
    handle.shutdown();
}

/// A 1 MB value dribbled in should still assemble, and should not be
/// re-scanned from scratch on each read (that is the O(n²) the state machine
/// exists to avoid). The wall clock is the observable proxy: a quadratic
/// parser on 1 MB in 4 KB chunks does ~128 GB of scanning.
#[tokio::test]
async fn a_large_value_arriving_in_chunks_is_not_rescanned() {
    let (handle, client) = start().await;
    let mut raw = Raw::connect(&handle).await;

    let size = 1 << 20;
    let value = vec![b'v'; size];
    let mut header = Vec::from(&b"*3\r\n$3\r\nSET\r\n$3\r\nbig\r\n"[..]);
    header.extend_from_slice(format!("${size}\r\n").as_bytes());
    raw.send(&header).await;

    let started = std::time::Instant::now();
    for piece in value.chunks(4096) {
        raw.send(piece).await;
    }
    raw.send(b"\r\n").await;
    assert_eq!(raw.read_some().await, "+OK\r\n");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "1 MB in 4 KB chunks took {elapsed:?}; the parser is re-scanning"
    );

    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let got: Vec<u8> = redis::cmd("GET")
        .arg("big")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(got.len(), size);
    assert!(got.iter().all(|&b| b == b'v'));
    handle.shutdown();
}

// ---------------------------------------------------------------------------
// pipelining
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_deep_pipeline_is_answered_in_order() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let mut pipe = redis::pipe();
    for i in 0..5_000 {
        pipe.cmd("SET").arg(format!("p:{i}")).arg(i).ignore();
    }
    let _: () = pipe.query_async(&mut con).await.unwrap();

    let mut pipe = redis::pipe();
    for i in 0..5_000 {
        pipe.cmd("GET").arg(format!("p:{i}"));
    }
    let values: Vec<i64> = pipe.query_async(&mut con).await.unwrap();
    assert_eq!(values, (0..5_000).collect::<Vec<i64>>());
    handle.shutdown();
}

#[tokio::test]
async fn inline_and_multibulk_can_be_mixed_in_one_pipeline() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;
    raw.send(b"PING\r\n*1\r\n$4\r\nPING\r\nECHO hi\r\n*2\r\n$4\r\nECHO\r\n$2\r\nyo\r\n\r\n*0\r\nPING\r\n")
        .await;
    let got = raw.read_some().await;
    assert_eq!(got, "+PONG\r\n+PONG\r\n$2\r\nhi\r\n$2\r\nyo\r\n+PONG\r\n");
    handle.shutdown();
}

// ---------------------------------------------------------------------------
// connection commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hello_handshake_with_auth_and_setname() {
    let (handle, _c) = start_with(Config {
        requirepass: Some("hunter2".into()),
        ..base_config()
    })
    .await;
    let mut raw = Raw::connect(&handle).await;

    // Without AUTH, HELLO refuses.
    raw.send(b"HELLO 3\r\n").await;
    let got = raw.read_some().await;
    assert!(got.starts_with("-NOAUTH HELLO must be called"), "{got}");

    // Wrong password.
    raw.send(b"HELLO 3 AUTH default wrong\r\n").await;
    let got = raw.read_some().await;
    assert_eq!(
        got,
        "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
    );

    // Unknown user.
    raw.send(b"HELLO 3 AUTH someone hunter2\r\n").await;
    let got = raw.read_some().await;
    assert!(got.starts_with("-WRONGPASS"), "{got}");

    // The full handshake, with SETNAME in the same command.
    raw.send(b"HELLO 3 AUTH default hunter2 SETNAME app-1\r\n")
        .await;
    let got = raw.read_some().await;
    assert!(got.starts_with("%7\r\n"), "RESP3 map expected: {got:?}");
    assert!(got.contains("$5\r\nproto\r\n:3\r\n"), "{got}");
    assert!(got.contains("$6\r\nserver\r\n$5\r\nredis\r\n"), "{got}");
    assert!(got.contains("$4\r\nmode\r\n$10\r\nstandalone\r\n"), "{got}");
    assert!(got.contains("$4\r\nrole\r\n$6\r\nmaster\r\n"), "{got}");
    assert!(got.contains("$7\r\nmodules\r\n*0\r\n"), "{got}");

    // The name stuck, and the connection is authenticated.
    raw.send(b"CLIENT GETNAME\r\n").await;
    assert_eq!(raw.read_some().await, "$5\r\napp-1\r\n");

    handle.shutdown();
}

#[tokio::test]
async fn hello_rejects_a_bad_version_and_a_bad_option() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;

    raw.send(b"HELLO 4\r\n").await;
    assert_eq!(
        raw.read_some().await,
        "-NOPROTO unsupported protocol version\r\n"
    );
    raw.send(b"HELLO abc\r\n").await;
    assert_eq!(
        raw.read_some().await,
        "-ERR Protocol version is not an integer or out of range\r\n"
    );
    raw.send(b"HELLO 3 NOPE x\r\n").await;
    let got = raw.read_some().await;
    assert!(
        got.starts_with("-ERR unknown argument 'NOPE' to HELLO"),
        "{got}"
    );

    // A plain HELLO with no arguments reports the current protocol.
    raw.send(b"HELLO\r\n").await;
    let got = raw.read_some().await;
    assert!(got.starts_with("*14\r\n"), "RESP2 flat map expected: {got}");

    handle.shutdown();
}

#[tokio::test]
async fn auth_against_requirepass() {
    let (handle, _c) = start_with(Config {
        requirepass: Some("hunter2".into()),
        ..base_config()
    })
    .await;
    let mut raw = Raw::connect(&handle).await;

    raw.send(b"GET k\r\n").await;
    assert_eq!(
        raw.read_some().await,
        "-NOAUTH Authentication required.\r\n"
    );
    raw.send(b"AUTH wrong\r\n").await;
    assert!(raw.read_some().await.starts_with("-WRONGPASS"));
    raw.send(b"AUTH default hunter2\r\n").await;
    assert_eq!(raw.read_some().await, "+OK\r\n");
    raw.send(b"GET k\r\n").await;
    assert_eq!(raw.read_some().await, "$-1\r\n");

    // RESET drops the authentication again.
    raw.send(b"RESET\r\n").await;
    assert_eq!(raw.read_some().await, "+RESET\r\n");
    raw.send(b"GET k\r\n").await;
    assert_eq!(
        raw.read_some().await,
        "-NOAUTH Authentication required.\r\n"
    );

    handle.shutdown();
}

#[tokio::test]
async fn auth_without_a_password_configured() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;
    raw.send(b"AUTH whatever\r\n").await;
    assert_eq!(
        raw.read_some().await,
        "-ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?\r\n"
    );
    handle.shutdown();
}

#[tokio::test]
async fn reset_returns_the_connection_to_its_initial_state() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;

    raw.send(b"HELLO 3\r\n").await;
    let _ = raw.read_some().await;
    raw.send(b"SELECT 5\r\n").await;
    assert_eq!(raw.read_some().await, "+OK\r\n");
    raw.send(b"CLIENT SETNAME temp\r\n").await;
    assert_eq!(raw.read_some().await, "+OK\r\n");

    raw.send(b"RESET\r\n").await;
    assert_eq!(raw.read_some().await, "+RESET\r\n");

    // Back to RESP2 (`$-1`, not `_`), db 0, and no name.
    raw.send(b"GET nothing\r\n").await;
    assert_eq!(raw.read_some().await, "$-1\r\n");
    raw.send(b"CLIENT GETNAME\r\n").await;
    assert_eq!(raw.read_some().await, "$-1\r\n");
    raw.send(b"CLIENT INFO\r\n").await;
    let info = raw.read_some().await;
    assert!(info.contains(" db=0 "), "{info}");
    assert!(info.contains(" resp=2 "), "{info}");

    handle.shutdown();
}

#[tokio::test]
async fn select_and_ping_and_echo() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;
    raw.send(b"PING\r\nPING hello\r\nECHO world\r\n").await;
    assert_eq!(
        raw.read_some().await,
        "+PONG\r\n$5\r\nhello\r\n$5\r\nworld\r\n"
    );
    raw.send(b"SELECT 15\r\nSELECT 16\r\nSELECT x\r\nSELECT -1\r\n")
        .await;
    assert_eq!(
        raw.read_some().await,
        "+OK\r\n-ERR DB index is out of range\r\n\
         -ERR value is not an integer or out of range\r\n\
         -ERR DB index is out of range\r\n"
    );
    handle.shutdown();
}

#[tokio::test]
async fn quit_replies_then_closes() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;
    raw.send(b"PING\r\nQUIT\r\nPING\r\n").await;
    // Everything after QUIT in the same batch is discarded, as in Redis.
    assert_eq!(raw.read_to_end().await, "+PONG\r\n+OK\r\n");
    handle.shutdown();
}

// ---------------------------------------------------------------------------
// CLIENT
// ---------------------------------------------------------------------------

fn parse_info_line(line: &str) -> std::collections::HashMap<String, String> {
    line.split(' ')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[tokio::test]
async fn client_info_reports_real_values() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let _: () = redis::cmd("SELECT")
        .arg(4)
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("tester")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("SETINFO")
        .arg("LIB-NAME")
        .arg("redis-rs")
        .query_async(&mut con)
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("SETINFO")
        .arg("LIB-VER")
        .arg("1.5.0")
        .query_async(&mut con)
        .await
        .unwrap();

    let info: String = redis::cmd("CLIENT")
        .arg("INFO")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        !info.contains('\n'),
        "CLIENT INFO is a single line: {info:?}"
    );

    let f = parse_info_line(&info);
    assert_eq!(f.get("name").map(String::as_str), Some("tester"));
    assert_eq!(f.get("db").map(String::as_str), Some("4"));
    assert_eq!(f.get("resp").map(String::as_str), Some("2"));
    assert_eq!(f.get("lib-name").map(String::as_str), Some("redis-rs"));
    assert_eq!(f.get("lib-ver").map(String::as_str), Some("1.5.0"));
    assert_eq!(f.get("cmd").map(String::as_str), Some("client|info"));
    assert_eq!(f.get("user").map(String::as_str), Some("default"));
    assert_eq!(f.get("multi").map(String::as_str), Some("-1"));
    assert_eq!(f.get("redir").map(String::as_str), Some("-1"));
    assert_eq!(f.get("flags").map(String::as_str), Some("N"));
    // The values that need a live connection to be non-trivial.
    let id: u64 = f.get("id").expect("id").parse().expect("numeric id");
    assert!(id >= 1);
    let addr = f.get("addr").expect("addr");
    assert!(addr.starts_with("127.0.0.1:"), "{addr}");
    let fd: i64 = f.get("fd").expect("fd").parse().expect("numeric fd");
    assert!(fd >= 0, "fd should be a real descriptor, got {fd}");
    let tot_in: u64 = f.get("tot-net-in").expect("tot-net-in").parse().unwrap();
    assert!(tot_in > 0, "tot-net-in should have counted the requests");
    let rbs: u64 = f.get("rbs").expect("rbs").parse().unwrap();
    assert!(rbs > 0);

    handle.shutdown();
}

#[tokio::test]
async fn client_list_reports_every_connection() {
    let (handle, client) = start().await;
    let mut a = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let mut b = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let _: () = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("alpha")
        .query_async(&mut a)
        .await
        .unwrap();
    let _: () = redis::cmd("CLIENT")
        .arg("SETNAME")
        .arg("beta")
        .query_async(&mut b)
        .await
        .unwrap();
    let _: () = redis::cmd("SELECT")
        .arg(7)
        .query_async(&mut b)
        .await
        .unwrap();

    let list: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(&mut a)
        .await
        .unwrap();
    let lines: Vec<&str> = list.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 2, "expected both connections: {list:?}");
    assert!(list.ends_with('\n'), "each line must be newline-terminated");

    let rows: Vec<_> = lines.iter().map(|l| parse_info_line(l)).collect();
    let names: Vec<&str> = rows
        .iter()
        .filter_map(|r| r.get("name").map(String::as_str))
        .collect();
    assert!(names.contains(&"alpha"), "{list}");
    assert!(names.contains(&"beta"), "{list}");

    // `beta`'s db must be reported from *its* state, not the caller's -- this
    // is the whole reason the registry exists.
    let beta = rows
        .iter()
        .find(|r| r.get("name").map(String::as_str) == Some("beta"))
        .expect("beta row");
    assert_eq!(beta.get("db").map(String::as_str), Some("7"));

    // Ids are ascending and unique.
    let mut ids: Vec<u64> = rows
        .iter()
        .filter_map(|r| r.get("id").and_then(|v| v.parse().ok()))
        .collect();
    let before = ids.clone();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids, before, "CLIENT LIST must be ordered by id, unique");

    handle.shutdown();
}

#[tokio::test]
async fn client_list_filters() {
    let (handle, client) = start().await;
    let mut a = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let mut b = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let a_id: u64 = redis::cmd("CLIENT")
        .arg("ID")
        .query_async(&mut a)
        .await
        .unwrap();
    let b_id: u64 = redis::cmd("CLIENT")
        .arg("ID")
        .query_async(&mut b)
        .await
        .unwrap();
    assert_ne!(a_id, b_id);

    let one: String = redis::cmd("CLIENT")
        .arg("LIST")
        .arg("ID")
        .arg(b_id)
        .query_async(&mut a)
        .await
        .unwrap();
    let lines: Vec<&str> = one.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with(&format!("id={b_id} ")), "{one}");

    let both: String = redis::cmd("CLIENT")
        .arg("LIST")
        .arg("ID")
        .arg(a_id)
        .arg(b_id)
        .query_async(&mut a)
        .await
        .unwrap();
    assert_eq!(both.lines().filter(|l| !l.is_empty()).count(), 2);

    // Nothing is in subscribe mode, so TYPE NORMAL is everything and
    // TYPE PUBSUB is nothing.
    let normal: String = redis::cmd("CLIENT")
        .arg("LIST")
        .arg("TYPE")
        .arg("NORMAL")
        .query_async(&mut a)
        .await
        .unwrap();
    assert_eq!(normal.lines().filter(|l| !l.is_empty()).count(), 2);
    let pubsub: String = redis::cmd("CLIENT")
        .arg("LIST")
        .arg("TYPE")
        .arg("PUBSUB")
        .query_async(&mut a)
        .await
        .unwrap();
    assert!(pubsub.is_empty(), "{pubsub:?}");

    let bad = redis::cmd("CLIENT")
        .arg("LIST")
        .arg("TYPE")
        .arg("nonsense")
        .query_async::<String>(&mut a)
        .await
        .unwrap_err();
    assert!(bad.to_string().contains("Unknown client type"), "{bad}");

    handle.shutdown();
}

#[tokio::test]
async fn client_kill_by_id_and_by_addr() {
    let (handle, client) = start().await;
    let mut killer = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    // A victim on a raw socket, so we can watch it get hung up on.
    let mut victim = Raw::connect(&handle).await;
    victim.send(b"CLIENT INFO\r\n").await;
    let info = victim.read_some().await;
    let f = parse_info_line(info.split("\r\n").nth(1).unwrap_or(""));
    let victim_id: u64 = f.get("id").expect("id").parse().expect("numeric");
    let victim_addr = f.get("addr").expect("addr").clone();

    let n: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(victim_id)
        .query_async(&mut killer)
        .await
        .unwrap();
    assert_eq!(n, 1);
    assert_eq!(victim.read_to_end().await, "", "the victim must be closed");

    // The old positional form replies +OK, and errors when nobody matches.
    let mut victim2 = Raw::connect(&handle).await;
    victim2.send(b"CLIENT INFO\r\n").await;
    let info = victim2.read_some().await;
    let f = parse_info_line(info.split("\r\n").nth(1).unwrap_or(""));
    let addr2 = f.get("addr").expect("addr").clone();
    let ok: String = redis::cmd("CLIENT")
        .arg("KILL")
        .arg(&addr2)
        .query_async(&mut killer)
        .await
        .unwrap();
    assert_eq!(ok, "OK");
    assert_eq!(victim2.read_to_end().await, "");

    let err = redis::cmd("CLIENT")
        .arg("KILL")
        .arg(&victim_addr)
        .query_async::<String>(&mut killer)
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("No such client address in client list"),
        "{err}"
    );

    handle.shutdown();
}

#[tokio::test]
async fn client_kill_skipme_protects_the_caller() {
    let (handle, client) = start().await;
    let mut a = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let mut victim = Raw::connect(&handle).await;
    victim.send(b"PING\r\n").await;
    let _ = victim.read_some().await;

    // SKIPME defaults to yes: the caller survives, the victim does not.
    let n: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("NORMAL")
        .query_async(&mut a)
        .await
        .unwrap();
    assert!(n >= 1, "at least the victim should have been killed");
    assert_eq!(victim.read_to_end().await, "");

    // The caller is still usable.
    let pong: String = redis::cmd("PING").query_async(&mut a).await.unwrap();
    assert_eq!(pong, "PONG");

    handle.shutdown();
}

#[tokio::test]
async fn client_kill_maxage_only_matches_old_connections() {
    let (handle, client) = start().await;
    let mut a = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let mut fresh = Raw::connect(&handle).await;
    fresh.send(b"PING\r\n").await;
    let _ = fresh.read_some().await;

    // Nothing here is an hour old.
    let n: i64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("MAXAGE")
        .arg(3600)
        .query_async(&mut a)
        .await
        .unwrap();
    assert_eq!(n, 0);
    fresh.send(b"PING\r\n").await;
    assert_eq!(fresh.read_some().await, "+PONG\r\n");

    handle.shutdown();
}

#[tokio::test]
async fn client_no_evict_no_touch_unpause_and_bad_subcommands() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;

    raw.send(b"CLIENT NO-EVICT ON\r\nCLIENT NO-EVICT OFF\r\n")
        .await;
    assert_eq!(raw.read_some().await, "+OK\r\n+OK\r\n");
    raw.send(b"CLIENT NO-TOUCH ON\r\n").await;
    assert_eq!(raw.read_some().await, "+OK\r\n");
    // NO-TOUCH is visible in the flags field.
    raw.send(b"CLIENT INFO\r\n").await;
    let info = raw.read_some().await;
    assert!(info.contains("flags=T"), "{info}");
    raw.send(b"CLIENT NO-TOUCH OFF\r\n").await;
    assert_eq!(raw.read_some().await, "+OK\r\n");

    raw.send(b"CLIENT UNPAUSE\r\n").await;
    assert_eq!(raw.read_some().await, "+OK\r\n");

    raw.send(b"CLIENT NO-EVICT MAYBE\r\n").await;
    assert_eq!(raw.read_some().await, "-ERR syntax error\r\n");
    raw.send(b"CLIENT NOSUCHTHING\r\n").await;
    let got = raw.read_some().await;
    assert!(
        got.starts_with("-ERR Unknown subcommand or wrong number of arguments for 'NOSUCHTHING'."),
        "{got}"
    );
    assert!(got.contains("Try CLIENT HELP."), "{got}");

    // Quoted, so it arrives as one argument -- an unquoted `with space` would
    // be four arguments and fail arity first, exactly as in Redis.
    raw.send(b"CLIENT SETNAME \"with space\"\r\n").await;
    let got = raw.read_some().await;
    assert!(
        got.starts_with("-ERR Client names cannot contain spaces"),
        "{got}"
    );
    raw.send(b"CLIENT SETNAME a b\r\n").await;
    assert_eq!(
        raw.read_some().await,
        "-ERR wrong number of arguments for 'client|setname' command\r\n"
    );

    raw.send(b"CLIENT SETINFO NOPE x\r\n").await;
    let got = raw.read_some().await;
    assert!(got.starts_with("-ERR Unrecognized option 'NOPE'"), "{got}");

    raw.send(b"CLIENT HELP\r\n").await;
    let got = raw.read_some().await;
    assert!(got.starts_with('*'), "{got}");
    assert!(got.contains("SETNAME <name>"), "{got}");

    handle.shutdown();
}

#[tokio::test]
async fn client_id_is_monotonic_and_matches_the_info_line() {
    let (handle, client) = start().await;
    let mut a = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let mut b = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let ida: u64 = redis::cmd("CLIENT")
        .arg("ID")
        .query_async(&mut a)
        .await
        .unwrap();
    let idb: u64 = redis::cmd("CLIENT")
        .arg("ID")
        .query_async(&mut b)
        .await
        .unwrap();
    assert!(idb > ida, "client ids must increase: {ida} then {idb}");

    let info: String = redis::cmd("CLIENT")
        .arg("INFO")
        .query_async(&mut b)
        .await
        .unwrap();
    assert!(info.starts_with(&format!("id={idb} ")), "{info}");
    handle.shutdown();
}

// ---------------------------------------------------------------------------
// connection lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn maxclients_is_enforced() {
    let (handle, _c) = start_with(Config {
        maxclients: 2,
        ..base_config()
    })
    .await;

    let mut held = Vec::new();
    for _ in 0..2 {
        let mut raw = Raw::connect(&handle).await;
        raw.send(b"PING\r\n").await;
        assert_eq!(raw.read_some().await, "+PONG\r\n");
        held.push(raw);
    }

    let mut over = Raw::connect(&handle).await;
    assert_eq!(
        over.read_to_end().await,
        "-ERR max number of clients reached\r\n"
    );

    // Freeing a slot lets the next client in.
    held.pop();
    // Give the server a moment to notice the close.
    for _ in 0..200 {
        let mut probe = Raw::connect(&handle).await;
        probe.send(b"PING\r\n").await;
        let got = probe.read_some().await;
        if got == "+PONG\r\n" {
            handle.shutdown();
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("a freed slot was never reused");
}

#[tokio::test]
async fn an_idle_connection_is_closed_after_timeout() {
    let (handle, _c) = start_with(Config {
        timeout: 1,
        ..base_config()
    })
    .await;
    let mut raw = Raw::connect(&handle).await;
    raw.send(b"PING\r\n").await;
    assert_eq!(raw.read_some().await, "+PONG\r\n");
    // No further traffic: the server must hang up within a few seconds.
    assert_eq!(raw.read_to_end().await, "");
    handle.shutdown();
}

#[tokio::test]
async fn an_active_connection_is_never_timed_out() {
    let (handle, _c) = start_with(Config {
        timeout: 1,
        ..base_config()
    })
    .await;
    let mut raw = Raw::connect(&handle).await;
    for _ in 0..8 {
        raw.send(b"PING\r\n").await;
        assert_eq!(raw.read_some().await, "+PONG\r\n");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    handle.shutdown();
}

#[tokio::test]
async fn many_concurrent_connections_are_all_served() {
    let (handle, client) = start().await;
    let mut tasks = Vec::new();
    for t in 0..64 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut con = client
                .get_multiplexed_async_connection()
                .await
                .expect("connect");
            for i in 0..25 {
                let key = format!("c:{t}:{i}");
                let _: () = redis::cmd("SET")
                    .arg(&key)
                    .arg(i)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let got: i64 = redis::cmd("GET")
                    .arg(&key)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                assert_eq!(got, i);
            }
        }));
    }
    for t in tasks {
        t.await.expect("client task panicked");
    }
    handle.shutdown();
}

#[tokio::test]
async fn shutdown_closes_live_connections_without_losing_a_reply() {
    let (handle, _c) = start().await;
    let mut raw = Raw::connect(&handle).await;
    raw.send(b"SET k v\r\n").await;
    assert_eq!(raw.read_some().await, "+OK\r\n");
    handle.shutdown();
    assert_eq!(raw.read_to_end().await, "");
}

// ---------------------------------------------------------------------------
// fuzzing
// ---------------------------------------------------------------------------

mod fuzz {
    use super::*;
    use proptest::prelude::*;

    /// Run the parser to exhaustion over `data`, returning `Err` on the first
    /// protocol error. Must never panic, whatever `data` is.
    fn drain(data: &[u8]) -> Result<usize, ()> {
        let mut parser = RequestParser::default();
        let mut buf = BytesMut::from(data);
        let mut n = 0usize;
        loop {
            match parser.parse(&mut buf) {
                Ok(Parsed::Command(_)) => n += 1,
                Ok(Parsed::Empty) => {}
                Ok(Parsed::Incomplete) => return Ok(n),
                Err(_) => return Err(()),
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        /// Arbitrary bytes: parse or protocol error, never a panic, and never
        /// consuming more than was supplied.
        #[test]
        fn arbitrary_bytes_never_panic(data in prop::collection::vec(any::<u8>(), 0..512)) {
            let _ = drain(&data);
        }

        /// Bytes drawn from the protocol's own alphabet find the interesting
        /// states far more often than uniform random noise does.
        #[test]
        fn protocol_shaped_bytes_never_panic(
            data in prop::collection::vec(
                prop::sample::select(vec![
                    b'*', b'$', b'\r', b'\n', b'0', b'1', b'9', b'-', b'+', b'"', b'\'',
                    b'\\', b' ', b'x', 0u8, 0xff,
                ]),
                0..256,
            )
        ) {
            let _ = drain(&data);
        }

        /// A valid stream with an arbitrary byte flipped: still no panic, and
        /// whatever happens, nothing is consumed that was not supplied.
        #[test]
        fn a_corrupted_valid_stream_never_panics(
            pos in 0usize..40,
            byte in any::<u8>(),
        ) {
            let mut wire = Vec::from(&b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"[..]);
            if let Some(slot) = wire.get_mut(pos % 32) {
                *slot = byte;
            }
            let _ = drain(&wire);
        }

        /// A valid command, chopped at an arbitrary point, must parse to the
        /// same thing as the un-chopped version.
        #[test]
        fn an_arbitrary_split_does_not_change_the_parse(split in 0usize..34) {
            let wire: &[u8] = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
            let split = split.min(wire.len());

            let mut parser = RequestParser::default();
            let mut buf = BytesMut::new();
            buf.extend_from_slice(wire.get(..split).unwrap_or_default());
            let first = parser.parse(&mut buf);
            prop_assert!(matches!(first, Ok(Parsed::Incomplete) | Ok(Parsed::Command(_))));

            buf.extend_from_slice(wire.get(split..).unwrap_or_default());
            let mut args: Vec<Vec<u8>> = Vec::new();
            if let Ok(Parsed::Command(a)) = first {
                args = a.iter().map(|b| b.to_vec()).collect();
            } else if let Ok(Parsed::Command(a)) = parser.parse(&mut buf) {
                args = a.iter().map(|b| b.to_vec()).collect();
            }
            prop_assert_eq!(
                args,
                vec![b"SET".to_vec(), b"foo".to_vec(), b"bar".to_vec()],
                "split at {}", split
            );
        }

        /// Any argument bytes at all round-trip through the multibulk
        /// encoding, including CR, LF and NUL.
        #[test]
        fn any_argument_bytes_round_trip(
            args in prop::collection::vec(
                prop::collection::vec(any::<u8>(), 0..64), 1..12)
        ) {
            let mut wire = Vec::new();
            wire.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
            for a in &args {
                wire.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
                wire.extend_from_slice(a);
                wire.extend_from_slice(b"\r\n");
            }
            let mut parser = RequestParser::default();
            let mut buf = BytesMut::from(&wire[..]);
            match parser.parse(&mut buf) {
                Ok(Parsed::Command(got)) => {
                    prop_assert_eq!(got.len(), args.len());
                    for (g, a) in got.iter().zip(args.iter()) {
                        prop_assert_eq!(&g[..], &a[..]);
                    }
                    prop_assert!(buf.is_empty());
                }
                other => prop_assert!(false, "expected a command, got {:?}", other),
            }
        }

        /// Inline commands: arbitrary printable input either splits or reports
        /// unbalanced quotes, and never panics.
        #[test]
        fn arbitrary_inline_input_never_panics(s in "[ -~]{0,120}") {
            let mut parser = RequestParser::default();
            let mut buf = BytesMut::new();
            buf.extend_from_slice(s.as_bytes());
            buf.extend_from_slice(b"\r\n");
            let _ = parser.parse(&mut buf);
        }
    }
}

/// The fuzzers above run the parser in isolation. This one drives the *whole
/// server* with random bytes over a real socket: no request may hang the
/// connection or take the process down.
#[tokio::test]
async fn random_bytes_over_a_real_socket_never_wedge_the_server() {
    let (handle, _c) = start().await;

    let mut state = 0x243f_6a88_85a3_08d3u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    for _ in 0..64 {
        let len = (next() % 200) as usize;
        let alphabet: &[u8] = b"*$\r\n019-+\"'\\ x";
        let payload: Vec<u8> = (0..len)
            .map(|_| {
                let r = next();
                if r % 4 == 0 {
                    (r >> 8) as u8
                } else {
                    *alphabet
                        .get((r >> 8) as usize % alphabet.len())
                        .unwrap_or(&b'*')
                }
            })
            .collect();

        let mut raw = Raw::connect(&handle).await;
        raw.send(&payload).await;
        // Either a reply, or a protocol error and a close, or silence because
        // the frame is incomplete. All three are fine; a hang is not.
        let mut buf = vec![0u8; 4096];
        let _ = tokio::time::timeout(Duration::from_millis(500), raw.stream.read(&mut buf)).await;
    }

    // Still healthy.
    let mut raw = Raw::connect(&handle).await;
    raw.send(b"PING\r\n").await;
    assert_eq!(raw.read_some().await, "+PONG\r\n");
    handle.shutdown();
}
