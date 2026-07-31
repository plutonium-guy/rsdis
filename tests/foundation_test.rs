//! End-to-end tests for the W0 foundation, driven by the real `redis` client
//! crate over a real TCP socket.
//!
//! Owner: F0. (`tests/**` belongs to W3d; §3 allows each agent to add
//! `tests/<own_area>_test.rs`.)
//!
//! These are deliberately black-box: they start the actual server, connect
//! with a third-party client, and assert on what comes back. Anything that
//! passes here also passes with `redis-cli`, because the client crate speaks
//! the same protocol.

use std::sync::Arc;

use redis::AsyncCommands;
use rsdis::config::Config;
use rsdis::ctx::ServerShared;
use rsdis::net::ServerHandle;

/// Start a server on an ephemeral port and return its handle plus a client.
async fn start() -> (ServerHandle, redis::Client) {
    let cfg = Config {
        // Port 0: let the kernel pick, so tests can run in parallel.
        port: 0,
        bind: vec!["127.0.0.1".to_string()],
        shard_count: 8,
        ..Default::default()
    };
    let server = ServerShared::new(cfg);
    let _ticker = server.spawn_clock_ticker();
    let handle = rsdis::net::serve(Arc::clone(&server))
        .await
        .expect("server must bind");
    let addr = handle.local_addr().expect("a bound address");
    let client = redis::Client::open(format!("redis://{addr}/")).expect("client must open");
    (handle, client)
}

#[tokio::test]
async fn ping_echo_and_set_get() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let pong: String = redis::cmd("PING").query_async(&mut con).await.unwrap();
    assert_eq!(pong, "PONG");

    let echoed: String = redis::cmd("ECHO")
        .arg("hello world")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(echoed, "hello world");

    let set: String = redis::cmd("SET")
        .arg("k")
        .arg("v")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(set, "OK");

    let got: String = con.get("k").await.unwrap();
    assert_eq!(got, "v");

    let missing: Option<String> = con.get("nope").await.unwrap();
    assert_eq!(missing, None);

    handle.shutdown();
}

#[tokio::test]
async fn del_exists_and_type() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let _: () = con.set("a", "1").await.unwrap();
    let _: () = con.set("b", "2").await.unwrap();

    let n: i64 = redis::cmd("EXISTS")
        .arg("a")
        .arg("b")
        .arg("missing")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(n, 2);

    let t: String = redis::cmd("TYPE")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(t, "string");

    let deleted: i64 = redis::cmd("DEL")
        .arg("a")
        .arg("b")
        .arg("missing")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(deleted, 2);

    let n: i64 = redis::cmd("EXISTS")
        .arg("a")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(n, 0);

    handle.shutdown();
}

#[tokio::test]
async fn select_isolates_databases() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let _: () = con.set("shared", "db0").await.unwrap();

    let ok: String = redis::cmd("SELECT")
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(ok, "OK");
    let v: Option<String> = con.get("shared").await.unwrap();
    assert_eq!(v, None);
    let _: () = con.set("shared", "db3").await.unwrap();

    let _: String = redis::cmd("SELECT")
        .arg(0)
        .query_async(&mut con)
        .await
        .unwrap();
    let v: String = con.get("shared").await.unwrap();
    assert_eq!(v, "db0");

    let err = redis::cmd("SELECT")
        .arg(99)
        .query_async::<String>(&mut con)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("DB index is out of range"),
        "unexpected error: {err}"
    );

    handle.shutdown();
}

#[tokio::test]
async fn hello_negotiates_resp3() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    // A RESP2 connection sees a flat array; the client turns it into a map.
    let reply: std::collections::HashMap<String, redis::Value> = redis::cmd("HELLO")
        .arg(3)
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        reply.get("server"),
        Some(&redis::Value::BulkString(b"redis".to_vec()))
    );
    assert_eq!(reply.get("proto"), Some(&redis::Value::Int(3)));
    assert!(reply.contains_key("version"));
    assert!(reply.contains_key("id"));
    assert!(reply.contains_key("mode"));
    assert!(reply.contains_key("role"));
    assert!(reply.contains_key("modules"));

    // The connection is RESP3 now; a missing key is `_` and the client still
    // maps it to None.
    let missing: Option<String> = con.get("nothing").await.unwrap();
    assert_eq!(missing, None);
    let _: () = con.set("k3", "v3").await.unwrap();
    let v: String = con.get("k3").await.unwrap();
    assert_eq!(v, "v3");

    let bad = redis::cmd("HELLO")
        .arg(9)
        .query_async::<redis::Value>(&mut con)
        .await
        .unwrap_err();
    assert!(
        bad.to_string().contains("unsupported protocol version"),
        "unexpected error: {bad}"
    );

    handle.shutdown();
}

/// The client negotiates RESP3 during its own handshake (`HELLO 3`), which is
/// the path a modern application actually takes. If `HELLO` were wrong, the
/// connection would fail to open at all.
#[tokio::test]
async fn resp3_connection_from_the_start() {
    let (handle, _client) = start().await;
    let addr = handle.local_addr().expect("a bound address");
    let client =
        redis::Client::open(format!("redis://{addr}/?protocol=resp3")).expect("client must open");
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let pong: String = redis::cmd("PING").query_async(&mut con).await.unwrap();
    assert_eq!(pong, "PONG");

    // On RESP3 a missing key is `_`, not `$-1`; the client must still see nil.
    let missing: Option<String> = con.get("nothing").await.unwrap();
    assert_eq!(missing, None);

    let _: () = con.set("k", "v").await.unwrap();
    let v: String = con.get("k").await.unwrap();
    assert_eq!(v, "v");

    // CONFIG GET is a genuine RESP3 map (`%n`), not a flat array.
    let cfg: std::collections::HashMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("databases")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(cfg.get("databases").map(String::as_str), Some("16"));

    handle.shutdown();
}

#[tokio::test]
async fn command_introspection() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let count: i64 = redis::cmd("COMMAND")
        .arg("COUNT")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(
        count > 10,
        "expected a populated command table, got {count}"
    );

    // COMMAND INFO GET must report Redis's own arity and key spec.
    let info: Vec<redis::Value> = redis::cmd("COMMAND")
        .arg("INFO")
        .arg("get")
        .query_async(&mut con)
        .await
        .unwrap();
    let redis::Value::Array(entry) = info.first().expect("one entry") else {
        panic!("expected an array entry, got {info:?}");
    };
    assert_eq!(entry.len(), 10);
    assert_eq!(
        entry.first(),
        Some(&redis::Value::BulkString(b"get".to_vec()))
    );
    assert_eq!(entry.get(1), Some(&redis::Value::Int(2)), "arity");
    assert_eq!(entry.get(3), Some(&redis::Value::Int(1)), "first_key");
    assert_eq!(entry.get(4), Some(&redis::Value::Int(1)), "last_key");
    assert_eq!(entry.get(5), Some(&redis::Value::Int(1)), "key_step");

    handle.shutdown();
}

#[tokio::test]
async fn pipelining_and_cross_shard_multikey() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    // MSET/MGET across keys that hash to different shards.
    let _: () = redis::cmd("MSET")
        .arg("alpha")
        .arg("1")
        .arg("beta")
        .arg("2")
        .arg("gamma")
        .arg("3")
        .query_async(&mut con)
        .await
        .unwrap();
    let got: Vec<Option<String>> = redis::cmd("MGET")
        .arg("gamma")
        .arg("alpha")
        .arg("missing")
        .arg("beta")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        got,
        vec![
            Some("3".to_string()),
            Some("1".to_string()),
            None,
            Some("2".to_string())
        ]
    );

    // A real pipeline: 200 writes then 200 reads, one round trip each way.
    let mut pipe = redis::pipe();
    for i in 0..200 {
        pipe.cmd("SET").arg(format!("p:{i}")).arg(i).ignore();
    }
    let _: () = pipe.query_async(&mut con).await.unwrap();

    let mut pipe = redis::pipe();
    for i in 0..200 {
        pipe.cmd("GET").arg(format!("p:{i}"));
    }
    let values: Vec<i64> = pipe.query_async(&mut con).await.unwrap();
    assert_eq!(values, (0..200).collect::<Vec<i64>>());

    let size: i64 = redis::cmd("DBSIZE").query_async(&mut con).await.unwrap();
    assert_eq!(size, 203);

    handle.shutdown();
}

#[tokio::test]
async fn set_options_and_expiry() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let first: Option<String> = redis::cmd("SET")
        .arg("nx")
        .arg("a")
        .arg("NX")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(first, Some("OK".to_string()));
    let second: Option<String> = redis::cmd("SET")
        .arg("nx")
        .arg("b")
        .arg("NX")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(second, None, "SET NX on an existing key replies nil");
    let v: String = con.get("nx").await.unwrap();
    assert_eq!(v, "a");

    let previous: String = redis::cmd("SET")
        .arg("nx")
        .arg("c")
        .arg("GET")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(previous, "a");

    // Expiry: set a 100 ms TTL and watch the key vanish.
    let _: () = redis::cmd("SET")
        .arg("ttl")
        .arg("v")
        .arg("PX")
        .arg(100)
        .query_async(&mut con)
        .await
        .unwrap();
    let pttl: i64 = redis::cmd("PTTL")
        .arg("ttl")
        .query_async(&mut con)
        .await
        .unwrap();
    assert!(pttl > 0 && pttl <= 100, "unexpected pttl {pttl}");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let gone: Option<String> = con.get("ttl").await.unwrap();
    assert_eq!(gone, None, "an expired key must read as missing");
    let pttl: i64 = redis::cmd("PTTL")
        .arg("ttl")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(pttl, -2, "PTTL on a missing key is -2");

    handle.shutdown();
}

#[tokio::test]
async fn error_strings_are_redis_compatible() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let e = redis::cmd("GET")
        .query_async::<redis::Value>(&mut con)
        .await
        .unwrap_err();
    assert!(
        e.to_string()
            .contains("wrong number of arguments for 'get' command"),
        "{e}"
    );

    let e = redis::cmd("NOSUCHCOMMAND")
        .arg("a")
        .query_async::<redis::Value>(&mut con)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("unknown command"), "{e}");

    let e = redis::cmd("SET")
        .arg("k")
        .arg("v")
        .arg("NX")
        .arg("XX")
        .query_async::<redis::Value>(&mut con)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("syntax error"), "{e}");

    handle.shutdown();
}

#[tokio::test]
async fn inline_commands_work() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (handle, _client) = start().await;
    let addr = handle.local_addr().unwrap();
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();

    // Two inline commands, pipelined, answered in one batch.
    s.write_all(b"PING\r\nECHO \"hi there\"\r\n").await.unwrap();
    let mut buf = vec![0u8; 128];
    let n = s.read(&mut buf).await.unwrap();
    let got = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(got.starts_with("+PONG\r\n"), "got {got:?}");
    assert!(got.contains("$8\r\nhi there\r\n"), "got {got:?}");

    handle.shutdown();
}

#[tokio::test]
async fn protocol_error_closes_the_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (handle, _client) = start().await;
    let addr = handle.local_addr().unwrap();
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();

    s.write_all(b"*1\r\nxbad\r\n").await.unwrap();
    let mut buf = Vec::new();
    s.read_to_end(&mut buf).await.unwrap();
    let got = String::from_utf8_lossy(&buf).into_owned();
    assert!(
        got.starts_with("-ERR Protocol error: expected '$', got 'x'"),
        "got {got:?}"
    );

    handle.shutdown();
}

#[tokio::test]
async fn quit_closes_cleanly() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let ok: String = redis::cmd("QUIT").query_async(&mut con).await.unwrap();
    assert_eq!(ok, "OK");
    handle.shutdown();
}

#[tokio::test]
async fn concurrent_clients_do_not_deadlock_or_corrupt() {
    let (handle, client) = start().await;

    // 16 clients hammering overlapping multi-key writes from opposite ends.
    let mut tasks = Vec::new();
    for t in 0..16 {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            let mut con = client
                .get_multiplexed_async_connection()
                .await
                .expect("connect");
            for i in 0..100 {
                let (a, b) = if t % 2 == 0 {
                    ("alpha", "omega")
                } else {
                    ("omega", "alpha")
                };
                let _: () = redis::cmd("MSET")
                    .arg(a)
                    .arg(i)
                    .arg(b)
                    .arg(i)
                    .query_async(&mut con)
                    .await
                    .unwrap();
                let _: Vec<Option<String>> = redis::cmd("MGET")
                    .arg(b)
                    .arg(a)
                    .query_async(&mut con)
                    .await
                    .unwrap();
            }
        }));
    }

    let all = futures_join(tasks);
    tokio::time::timeout(std::time::Duration::from_secs(60), all)
        .await
        .expect("clients deadlocked");

    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let pong: String = redis::cmd("PING").query_async(&mut con).await.unwrap();
    assert_eq!(pong, "PONG");

    handle.shutdown();
}

async fn futures_join(tasks: Vec<tokio::task::JoinHandle<()>>) {
    for t in tasks {
        t.await.expect("client task panicked");
    }
}

#[tokio::test]
async fn info_reports_a_parsable_document() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");
    let info: String = redis::cmd("INFO").query_async(&mut con).await.unwrap();
    assert!(info.contains("redis_version:"), "{info}");
    assert!(info.contains("# Server"), "{info}");
    assert!(info.contains("# Keyspace"), "{info}");
    handle.shutdown();
}

#[tokio::test]
async fn config_get_and_set() {
    let (handle, client) = start().await;
    let mut con = client
        .get_multiplexed_async_connection()
        .await
        .expect("connect");

    let cfg: std::collections::HashMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory-policy")
        .arg("hash-max-listpack-entries")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        cfg.get("maxmemory-policy").map(String::as_str),
        Some("noeviction")
    );
    assert_eq!(
        cfg.get("hash-max-listpack-entries").map(String::as_str),
        Some("128")
    );

    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("maxmemory-policy")
        .arg("allkeys-lru")
        .query_async(&mut con)
        .await
        .unwrap();
    let cfg: std::collections::HashMap<String, String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("maxmemory-policy")
        .query_async(&mut con)
        .await
        .unwrap();
    assert_eq!(
        cfg.get("maxmemory-policy").map(String::as_str),
        Some("allkeys-lru")
    );

    handle.shutdown();
}
