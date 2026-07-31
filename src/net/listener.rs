//! Listeners and the accept loop.
//!
//! Owned by W1b; do not edit if you are not that agent.
//!
//! §2.2: one accept loop per worker, over `SO_REUSEPORT` sockets, so the
//! kernel spreads incoming connections across workers instead of funnelling
//! them through a single accept queue.

use std::io;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use crate::ctx::ServerShared;
use crate::info::Stats;

/// A running server.
pub struct ServerHandle {
    /// Every address actually bound. Useful in tests, where port 0 is used.
    pub addrs: Vec<SocketAddr>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    server: Arc<ServerShared>,
}

impl ServerHandle {
    /// The first bound address. Tests use this after binding port 0.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.addrs.first().copied()
    }

    /// Ask every accept loop to stop and drop the listeners.
    pub fn shutdown(self) {
        self.server
            .shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
        for t in self.tasks {
            t.abort();
        }
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

    if !bound_any {
        return Err(first_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::AddrNotAvailable, "no bind address available")
        }));
    }

    info!(?addrs, workers, "rsdis ready to accept connections");
    Ok(ServerHandle {
        addrs,
        tasks,
        server,
    })
}

async fn accept_loop(server: Arc<ServerShared>, listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                // §2.2: disable Nagle. A reply must not wait for more data.
                if let Err(e) = stream.set_nodelay(true) {
                    warn!(error = %e, "TCP_NODELAY failed");
                }
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
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}
