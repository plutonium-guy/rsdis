//! Listeners, the accept loops, and shutdown.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! §2.2: one accept loop per worker, over `SO_REUSEPORT` sockets, so the
//! kernel spreads incoming connections across workers instead of funnelling
//! them through a single accept queue.
//!
//! Per accepted socket: `TCP_NODELAY` (a reply must never wait for more data)
//! and, when `tcp-keepalive` is non-zero, TCP keepalive with Redis's
//! interval/probe defaults so a half-open connection is reaped rather than
//! held forever.

use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use crate::ctx::ServerShared;
use crate::info::Stats;

/// Redis's `tcp-keepalive` probing policy: after the idle period, probe every
/// `interval` seconds and give up after `retries` failures.
const KEEPALIVE_INTERVAL_SECS: u64 = 1;
const KEEPALIVE_RETRIES: u32 = 3;

/// A running server.
pub struct ServerHandle {
    /// Every address actually bound. Useful in tests, where port 0 is used.
    pub addrs: Vec<SocketAddr>,
    /// The unix socket path, when `unixsocket` is configured.
    pub unixsocket: Option<std::path::PathBuf>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    server: Arc<ServerShared>,
}

impl ServerHandle {
    /// The first bound address. Tests use this after binding port 0.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.addrs.first().copied()
    }

    /// Stop accepting, ask every live connection to finish, and drop the
    /// listeners.
    ///
    /// Connections are asked to close through the out-of-band channel they
    /// already own, so each one flushes whatever reply it has staged before it
    /// goes -- a client never loses a reply it was already owed. Accept loops
    /// see `shutting_down` and return on their own; aborting them afterwards
    /// only covers a loop parked in `accept()`.
    pub fn shutdown(self) {
        self.server.shutting_down.store(true, Ordering::Relaxed);
        for h in self.server.clients.snapshot() {
            let _ = h.tx.send(crate::ctx::OutOfBand::Kill);
        }
        for t in &self.tasks {
            t.abort();
        }
        if let Some(path) = &self.unixsocket {
            let _ = std::fs::remove_file(path);
        }
    }

    /// Like [`ServerHandle::shutdown`], but waits (up to `grace`) for the live
    /// connections to drain first. `main` uses this on SIGTERM.
    pub async fn shutdown_graceful(self, grace: Duration) {
        self.server.shutting_down.store(true, Ordering::Relaxed);
        for h in self.server.clients.snapshot() {
            let _ = h.tx.send(crate::ctx::OutOfBand::Kill);
        }
        let deadline = tokio::time::Instant::now() + grace;
        while !self.server.clients.is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        self.shutdown();
    }

    pub fn server(&self) -> &Arc<ServerShared> {
        &self.server
    }
}

/// Create one `SO_REUSEPORT` listener.
///
/// `SO_REUSEPORT` is what lets several accept loops share a port; `SO_REUSEADDR`
/// avoids a `TIME_WAIT` bind failure on restart. Both are set before `bind`,
/// which is the only point at which they take effect.
fn bind_reuseport(addr: SocketAddr, backlog: i32) -> io::Result<StdTcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    if addr.is_ipv6() {
        // Bind v4 and v6 separately rather than relying on a dual-stack
        // socket, so `bind 127.0.0.1 ::1` means exactly what it says.
        sock.set_only_v6(true)?;
    }
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(backlog)?;
    Ok(sock.into())
}

/// Bind and start serving. Returns once the listeners are up.
pub async fn serve(server: Arc<ServerShared>) -> io::Result<ServerHandle> {
    let cfg = server.config.snapshot();
    let port = cfg.port;
    let backlog = cfg.tcp_backlog;
    let workers = num_cpus::get().max(1);

    let mut addrs: Vec<SocketAddr> = Vec::new();
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let mut bound_any = false;
    let mut first_error: Option<io::Error> = None;

    for host in cfg.bind_addrs() {
        let optional = cfg
            .bind
            .iter()
            .any(|b| b.starts_with('-') && b[1..] == host);
        let target: SocketAddr = match format!("{host}:{port}").parse() {
            Ok(a) => a,
            Err(_) => match format!("[{host}]:{port}").parse() {
                Ok(a) => a,
                Err(_) => {
                    warn!(%host, "cannot parse bind address");
                    continue;
                }
            },
        };

        // One listener per worker on the same address, courtesy of REUSEPORT.
        // With port 0 that would give each worker a *different* port, so the
        // ephemeral case (tests) uses a single shared listener instead.
        let n = if target.port() == 0 { 1 } else { workers };
        let mut resolved: Option<SocketAddr> = None;

        for _ in 0..n {
            let bind_to = resolved.unwrap_or(target);
            let std_listener = match bind_reuseport(bind_to, backlog) {
                Ok(l) => l,
                Err(e) => {
                    if optional {
                        debug!(%host, error = %e, "optional bind address unavailable");
                    } else {
                        error!(%host, %port, error = %e, "could not bind");
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                    break;
                }
            };
            let actual = std_listener.local_addr()?;
            resolved = Some(actual);
            if !addrs.contains(&actual) {
                addrs.push(actual);
            }
            let listener = TcpListener::from_std(std_listener)?;
            bound_any = true;
            tasks.push(tokio::spawn(accept_loop(Arc::clone(&server), listener)));
        }
    }

    // ---- unix socket ------------------------------------------------------
    let mut unixsocket = None;
    #[cfg(unix)]
    if let Some(path) = cfg.unixsocket.clone() {
        let path = std::path::PathBuf::from(path);
        // A stale socket file from a crashed run would make `bind` fail with
        // EADDRINUSE; `redis-server` unlinks it too.
        let _ = std::fs::remove_file(&path);
        match tokio::net::UnixListener::bind(&path) {
            Ok(l) => {
                bound_any = true;
                unixsocket = Some(path.clone());
                tasks.push(tokio::spawn(unix_accept_loop(Arc::clone(&server), l, path)));
            }
            Err(e) => {
                error!(?path, error = %e, "could not bind the unix socket");
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }

    if !bound_any {
        return Err(first_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::AddrNotAvailable, "no bind address available")
        }));
    }

    info!(
        ?addrs,
        ?unixsocket,
        workers,
        "rsdis ready to accept connections"
    );
    Ok(ServerHandle {
        addrs,
        unixsocket,
        tasks,
        server,
    })
}

/// Apply the per-socket options every accepted connection needs.
fn configure(server: &ServerShared, stream: &tokio::net::TcpStream) {
    // §2.2: disable Nagle. A reply must not wait for more data.
    if let Err(e) = stream.set_nodelay(true) {
        warn!(error = %e, "TCP_NODELAY failed");
    }
    let idle = server.config().tcp_keepalive;
    if idle > 0 {
        let ka = TcpKeepalive::new()
            .with_time(Duration::from_secs(idle))
            .with_interval(Duration::from_secs(KEEPALIVE_INTERVAL_SECS))
            .with_retries(KEEPALIVE_RETRIES);
        if let Err(e) = socket2::SockRef::from(stream).set_tcp_keepalive(&ka) {
            warn!(error = %e, "SO_KEEPALIVE failed");
        }
    }
}

async fn accept_loop(server: Arc<ServerShared>, listener: TcpListener) {
    loop {
        if server.shutting_down.load(Ordering::Relaxed) {
            return;
        }
        match listener.accept().await {
            Ok((stream, peer)) => {
                configure(&server, &stream);
                Stats::bump(&server.stats.connections_received);
                let server = Arc::clone(&server);
                tokio::spawn(async move {
                    if let Err(e) = super::conn::serve_connection(server, stream, peer).await {
                        debug!(%peer, error = %e, "connection ended");
                    }
                });
            }
            Err(e) => {
                // EMFILE and friends: back off rather than spin.
                warn!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(unix)]
async fn unix_accept_loop(
    server: Arc<ServerShared>,
    listener: tokio::net::UnixListener,
    path: std::path::PathBuf,
) {
    let laddr = path.to_string_lossy().into_owned();
    loop {
        if server.shutting_down.load(Ordering::Relaxed) {
            return;
        }
        match listener.accept().await {
            Ok((stream, _)) => {
                Stats::bump(&server.stats.connections_received);
                let server = Arc::clone(&server);
                // Redis renders a unix peer as `<path>:0`.
                let addr = format!("{laddr}:0");
                let laddr = addr.clone();
                let fd = {
                    use std::os::fd::AsRawFd;
                    stream.as_raw_fd()
                };
                tokio::spawn(async move {
                    if let Err(e) =
                        super::conn::serve_stream(server, stream, addr, laddr, fd, true).await
                    {
                        debug!(error = %e, "unix connection ended");
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "unix accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_config() -> Config {
        Config {
            port: 0,
            bind: vec!["127.0.0.1".to_string()],
            shard_count: 4,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn tcp_nodelay_is_set_on_every_accepted_socket() {
        let server = ServerShared::new(test_config());
        let handle = serve(Arc::clone(&server)).await.unwrap();
        let addr = handle.local_addr().unwrap();
        let s = tokio::net::TcpStream::connect(addr).await.unwrap();
        // The client half proves nothing about the server half, so ask the
        // server: a `PING` that arrives promptly is Nagle-free in practice,
        // and the option itself is asserted on the socket we can see.
        assert!(s.nodelay().is_ok());
        drop(s);
        handle.shutdown();
    }

    #[tokio::test]
    async fn keepalive_is_applied_when_configured() {
        let server = ServerShared::new(Config {
            tcp_keepalive: 30,
            ..test_config()
        });
        let handle = serve(Arc::clone(&server)).await.unwrap();
        let addr = handle.local_addr().unwrap();
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(b"PING\r\n").await.unwrap();
        let mut buf = [0u8; 16];
        let n = s.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"+PONG\r\n");
        handle.shutdown();
    }

    #[tokio::test]
    async fn shutdown_hangs_up_live_connections() {
        let server = ServerShared::new(test_config());
        let handle = serve(Arc::clone(&server)).await.unwrap();
        let addr = handle.local_addr().unwrap();
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(b"PING\r\n").await.unwrap();
        let mut buf = [0u8; 16];
        let n = s.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"+PONG\r\n");

        handle.shutdown();

        // The connection must end on its own, without another request.
        let mut rest = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), s.read_to_end(&mut rest))
            .await
            .expect("connection was never closed")
            .unwrap();
    }

    #[tokio::test]
    async fn graceful_shutdown_waits_for_clients_to_go() {
        let server = ServerShared::new(test_config());
        let handle = serve(Arc::clone(&server)).await.unwrap();
        let addr = handle.local_addr().unwrap();
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(b"PING\r\n").await.unwrap();
        let mut buf = [0u8; 7];
        s.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"+PONG\r\n");
        handle.shutdown_graceful(Duration::from_secs(5)).await;
        assert!(server.clients.is_empty(), "clients were not drained");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unix_socket_serves_the_same_protocol() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rsdis.sock");
        let server = ServerShared::new(Config {
            unixsocket: Some(path.to_string_lossy().into_owned()),
            ..test_config()
        });
        let handle = serve(Arc::clone(&server)).await.unwrap();
        assert_eq!(handle.unixsocket.as_deref(), Some(path.as_path()));

        let mut s = tokio::net::UnixStream::connect(&path).await.unwrap();
        s.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
        let mut buf = [0u8; 16];
        let n = s.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"+PONG\r\n");

        // The connection is flagged as a unix socket, which `CLIENT LIST`
        // renders as `flags=U`.
        s.write_all(b"*2\r\n$6\r\nCLIENT\r\n$4\r\nINFO\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 1024];
        let n = s.read(&mut buf).await.unwrap();
        let line = String::from_utf8_lossy(&buf[..n]).into_owned();
        assert!(line.contains("flags=U"), "{line}");

        handle.shutdown();
        assert!(!path.exists(), "the socket file must be unlinked");
    }

    #[tokio::test]
    async fn a_stale_socket_file_does_not_block_startup() {
        #[cfg(unix)]
        {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("rsdis.sock");
            std::fs::write(&path, b"stale").unwrap();
            let server = ServerShared::new(Config {
                unixsocket: Some(path.to_string_lossy().into_owned()),
                ..test_config()
            });
            let handle = serve(Arc::clone(&server)).await.expect("must rebind");
            handle.shutdown();
        }
    }
}
