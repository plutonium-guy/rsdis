//! The `rsdis` binary.
//!
//! Owner: F0.
//!
//! Usage matches `redis-server`:
//!
//! ```text
//! rsdis [/path/to/redis.conf] [--directive value ...]
//! ```

use std::process::ExitCode;
use std::sync::Arc;

use rsdis::config::Cli;
use rsdis::ctx::ServerShared;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    let cli = Cli::from_env();

    let cfg = match cli.into_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("*** FATAL CONFIG FILE ERROR ***");
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    init_tracing(&cfg.loglevel);

    // §2.2: worker count = core count.
    let workers = num_cpus::get().max(1);
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .thread_name("rsdis-worker")
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("could not start the tokio runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    let port = cfg.port;
    let shards = cfg.effective_shard_count();

    runtime.block_on(async move {
        let server = ServerShared::new(cfg);
        info!(
            version = rsdis::VERSION,
            pid = server.pid,
            port,
            shards,
            workers,
            run_id = %server.run_id,
            "rsdis starting"
        );

        // §5.6: the coarse clock.
        let _ticker = server.spawn_clock_ticker();

        let handle = match rsdis::net::serve(Arc::clone(&server)).await {
            Ok(h) => h,
            Err(e) => {
                error!(error = %e, "could not bind; exiting");
                return ExitCode::FAILURE;
            }
        };

        wait_for_shutdown().await;
        info!("shutting down");
        handle.shutdown();
        ExitCode::SUCCESS
    })
}

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn init_tracing(loglevel: &str) {
    let default = match loglevel {
        "debug" | "verbose" => "debug",
        "warning" => "warn",
        "nothing" => "off",
        _ => "info",
    };
    let filter = EnvFilter::try_from_env("RSDIS_LOG")
        .unwrap_or_else(|_| EnvFilter::new(format!("rsdis={default}")));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}
