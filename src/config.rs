//! Server configuration.
//!
//! Owner: F0.
//!
//! The whole Redis config surface lives in one struct with real Redis 7.4
//! defaults. It is deliberately over-provisioned: six agents are about to work
//! in parallel worktrees, and a directive that is missing here becomes a merge
//! conflict in a shared file. If you need a directive that is not here, that
//! is a contract gap -- report it.
//!
//! Reads on the hot path go through an [`ArcSwap`] snapshot
//! (`ServerShared::config()`), never a lock: `CONFIG SET` swaps a whole new
//! `Arc<Config>` in, and in-flight commands keep using the snapshot they
//! started with.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use clap::Parser;

use crate::notify::NotifyClass;

/// `maxmemory-policy`, all eight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MaxmemoryPolicy {
    #[default]
    NoEviction,
    AllkeysLru,
    AllkeysLfu,
    AllkeysRandom,
    VolatileLru,
    VolatileLfu,
    VolatileRandom,
    VolatileTtl,
}

impl MaxmemoryPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "noeviction" => MaxmemoryPolicy::NoEviction,
            "allkeys-lru" => MaxmemoryPolicy::AllkeysLru,
            "allkeys-lfu" => MaxmemoryPolicy::AllkeysLfu,
            "allkeys-random" => MaxmemoryPolicy::AllkeysRandom,
            "volatile-lru" => MaxmemoryPolicy::VolatileLru,
            "volatile-lfu" => MaxmemoryPolicy::VolatileLfu,
            "volatile-random" => MaxmemoryPolicy::VolatileRandom,
            "volatile-ttl" => MaxmemoryPolicy::VolatileTtl,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            MaxmemoryPolicy::NoEviction => "noeviction",
            MaxmemoryPolicy::AllkeysLru => "allkeys-lru",
            MaxmemoryPolicy::AllkeysLfu => "allkeys-lfu",
            MaxmemoryPolicy::AllkeysRandom => "allkeys-random",
            MaxmemoryPolicy::VolatileLru => "volatile-lru",
            MaxmemoryPolicy::VolatileLfu => "volatile-lfu",
            MaxmemoryPolicy::VolatileRandom => "volatile-random",
            MaxmemoryPolicy::VolatileTtl => "volatile-ttl",
        }
    }

    /// True when `Entry::lru` holds an LFU counter rather than an LRU clock.
    #[inline]
    pub fn is_lfu(self) -> bool {
        matches!(
            self,
            MaxmemoryPolicy::AllkeysLfu | MaxmemoryPolicy::VolatileLfu
        )
    }
}

/// `appendfsync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AppendFsync {
    Always,
    #[default]
    EverySec,
    No,
}

impl AppendFsync {
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "always" => AppendFsync::Always,
            "everysec" => AppendFsync::EverySec,
            "no" => AppendFsync::No,
            _ => return None,
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            AppendFsync::Always => "always",
            AppendFsync::EverySec => "everysec",
            AppendFsync::No => "no",
        }
    }
}

/// One `save <seconds> <changes>` point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SavePoint {
    pub seconds: u64,
    pub changes: u64,
}

/// Everything the server is configured with.
#[derive(Debug, Clone)]
pub struct Config {
    // ---- network -----------------------------------------------------------
    pub port: u16,
    pub bind: Vec<String>,
    pub unixsocket: Option<String>,
    pub tcp_backlog: i32,
    pub tcp_keepalive: u64,
    pub timeout: u64,
    pub maxclients: usize,
    pub proto_max_bulk_len: u64,

    // ---- keyspace ----------------------------------------------------------
    pub databases: usize,
    /// Number of keyspace shards. `0` means "derive from the core count",
    /// which is what `ServerShared::new` does.
    pub shard_count: usize,

    // ---- memory ------------------------------------------------------------
    pub maxmemory: u64,
    pub maxmemory_policy: MaxmemoryPolicy,
    pub maxmemory_samples: u32,
    pub maxmemory_eviction_tenacity: u32,
    pub lfu_log_factor: u32,
    pub lfu_decay_time: u32,

    // ---- encoding thresholds (§5.5) ---------------------------------------
    pub hash_max_listpack_entries: usize,
    pub hash_max_listpack_value: usize,
    pub list_max_listpack_size: i64,
    pub list_compress_depth: i32,
    pub set_max_intset_entries: usize,
    pub set_max_listpack_entries: usize,
    pub set_max_listpack_value: usize,
    pub zset_max_listpack_entries: usize,
    pub zset_max_listpack_value: usize,
    pub hll_sparse_max_bytes: usize,
    pub stream_node_max_entries: usize,
    pub stream_node_max_bytes: usize,

    // ---- persistence -------------------------------------------------------
    pub dir: PathBuf,
    pub dbfilename: String,
    pub save: Vec<SavePoint>,
    pub rdbcompression: bool,
    pub rdbchecksum: bool,
    pub stop_writes_on_bgsave_error: bool,
    pub appendonly: bool,
    pub appendfilename: String,
    pub appenddirname: String,
    pub appendfsync: AppendFsync,
    pub no_appendfsync_on_rewrite: bool,
    pub auto_aof_rewrite_percentage: u32,
    pub auto_aof_rewrite_min_size: u64,
    pub aof_use_rdb_preamble: bool,
    pub aof_timestamp_enabled: bool,

    // ---- security ----------------------------------------------------------
    pub requirepass: Option<String>,

    // ---- notifications -----------------------------------------------------
    pub notify_keyspace_events: NotifyClass,

    // ---- introspection -----------------------------------------------------
    pub loglevel: String,
    pub logfile: String,
    pub slowlog_log_slower_than: i64,
    pub slowlog_max_len: usize,
    pub latency_monitor_threshold: u64,
    pub latency_tracking: bool,

    // ---- misc --------------------------------------------------------------
    pub activerehashing: bool,
    pub activeexpire: bool,
    pub lazyfree_lazy_expire: bool,
    pub lazyfree_lazy_eviction: bool,
    pub lazyfree_lazy_server_del: bool,
    pub lazyfree_lazy_user_del: bool,
    pub lazyfree_lazy_user_flush: bool,
    pub io_threads: usize,
    pub enable_protected_configs: bool,
    pub enable_debug_command: bool,
    pub daemonize: bool,
    pub pidfile: Option<PathBuf>,
    pub set_proc_title: bool,
}

impl Default for Config {
    /// Real Redis 7.4 defaults. Every value here was cross-checked against
    /// `config.c`'s `standardConfig static_configs[]` table.
    fn default() -> Self {
        Config {
            port: 6379,
            bind: vec!["127.0.0.1".to_string(), "-::1".to_string()],
            unixsocket: None,
            tcp_backlog: 511,
            tcp_keepalive: 300,
            timeout: 0,
            maxclients: 10_000,
            proto_max_bulk_len: 512 * 1024 * 1024,

            databases: 16,
            shard_count: 0,

            maxmemory: 0,
            maxmemory_policy: MaxmemoryPolicy::NoEviction,
            maxmemory_samples: 5,
            maxmemory_eviction_tenacity: 10,
            lfu_log_factor: 10,
            lfu_decay_time: 1,

            hash_max_listpack_entries: 128,
            hash_max_listpack_value: 64,
            list_max_listpack_size: 128,
            list_compress_depth: 0,
            set_max_intset_entries: 512,
            set_max_listpack_entries: 128,
            set_max_listpack_value: 64,
            zset_max_listpack_entries: 128,
            zset_max_listpack_value: 64,
            hll_sparse_max_bytes: 3000,
            stream_node_max_entries: 100,
            stream_node_max_bytes: 4096,

            dir: PathBuf::from("."),
            dbfilename: "dump.rdb".to_string(),
            save: vec![
                SavePoint {
                    seconds: 3600,
                    changes: 1,
                },
                SavePoint {
                    seconds: 300,
                    changes: 100,
                },
                SavePoint {
                    seconds: 60,
                    changes: 10_000,
                },
            ],
            rdbcompression: true,
            rdbchecksum: true,
            stop_writes_on_bgsave_error: true,
            appendonly: false,
            appendfilename: "appendonly.aof".to_string(),
            appenddirname: "appendonlydir".to_string(),
            appendfsync: AppendFsync::EverySec,
            no_appendfsync_on_rewrite: false,
            auto_aof_rewrite_percentage: 100,
            auto_aof_rewrite_min_size: 64 * 1024 * 1024,
            aof_use_rdb_preamble: true,
            aof_timestamp_enabled: false,

            requirepass: None,

            notify_keyspace_events: NotifyClass::empty(),

            loglevel: "notice".to_string(),
            logfile: String::new(),
            slowlog_log_slower_than: 10_000,
            slowlog_max_len: 128,
            latency_monitor_threshold: 0,
            latency_tracking: true,

            activerehashing: true,
            activeexpire: true,
            lazyfree_lazy_expire: false,
            lazyfree_lazy_eviction: false,
            lazyfree_lazy_server_del: false,
            lazyfree_lazy_user_del: false,
            lazyfree_lazy_user_flush: false,
            io_threads: 1,
            enable_protected_configs: false,
            enable_debug_command: false,
            daemonize: false,
            pidfile: None,
            set_proc_title: true,
        }
    }
}

/// A configuration error, with the offending directive.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Bad directive or wrong number of arguments")]
    BadDirective,
    #[error("argument couldn't be parsed into an integer")]
    NotAnInteger,
    #[error("argument must be 'yes' or 'no'")]
    NotABoolean,
    #[error("{0}")]
    Invalid(String),
    #[error("Unknown directive '{0}'")]
    Unknown(String),
    #[error("Can't open the config file: {0}")]
    Io(#[from] std::io::Error),
}

impl Config {
    /// Apply one `name value...` directive, as it would appear in redis.conf
    /// or after `CONFIG SET`.
    ///
    /// Unknown directives are reported rather than ignored: silently dropping
    /// `maxmemory-polciy` is exactly the kind of bug that only shows up in
    /// production.
    pub fn apply(&mut self, name: &str, args: &[String]) -> Result<(), ConfigError> {
        let one = || -> Result<&str, ConfigError> {
            args.first()
                .map(String::as_str)
                .ok_or(ConfigError::BadDirective)
        };
        let int = |s: &str| -> Result<i64, ConfigError> {
            s.parse::<i64>().map_err(|_| ConfigError::NotAnInteger)
        };
        let mem = |s: &str| -> Result<u64, ConfigError> {
            parse_memory(s).ok_or(ConfigError::NotAnInteger)
        };
        let boolean = |s: &str| -> Result<bool, ConfigError> {
            match s.to_ascii_lowercase().as_str() {
                "yes" | "true" | "1" => Ok(true),
                "no" | "false" | "0" => Ok(false),
                _ => Err(ConfigError::NotABoolean),
            }
        };
        let uint = |s: &str| -> Result<u64, ConfigError> {
            let v = int(s)?;
            u64::try_from(v).map_err(|_| ConfigError::NotAnInteger)
        };
        let usz = |s: &str| -> Result<usize, ConfigError> {
            let v = int(s)?;
            usize::try_from(v).map_err(|_| ConfigError::NotAnInteger)
        };

        match name.to_ascii_lowercase().as_str() {
            // network
            "port" => {
                self.port = u16::try_from(int(one()?)?).map_err(|_| ConfigError::NotAnInteger)?
            }
            "bind" => {
                if args.is_empty() {
                    return Err(ConfigError::BadDirective);
                }
                self.bind = args.to_vec();
            }
            "unixsocket" => self.unixsocket = Some(one()?.to_string()),
            "tcp-backlog" => {
                self.tcp_backlog =
                    i32::try_from(int(one()?)?).map_err(|_| ConfigError::NotAnInteger)?
            }
            "tcp-keepalive" => self.tcp_keepalive = uint(one()?)?,
            "timeout" => self.timeout = uint(one()?)?,
            "maxclients" => self.maxclients = usz(one()?)?,
            "proto-max-bulk-len" => self.proto_max_bulk_len = mem(one()?)?,

            // keyspace
            "databases" => {
                let n = usz(one()?)?;
                if n == 0 {
                    return Err(ConfigError::Invalid("databases must be >= 1".into()));
                }
                self.databases = n;
            }
            "shard-count" => self.shard_count = usz(one()?)?,

            // memory
            "maxmemory" => self.maxmemory = mem(one()?)?,
            "maxmemory-policy" => {
                self.maxmemory_policy = MaxmemoryPolicy::parse(one()?).ok_or_else(|| {
                    ConfigError::Invalid(format!("invalid policy '{}'", one().unwrap_or("")))
                })?
            }
            "maxmemory-samples" => {
                self.maxmemory_samples = u32::try_from(uint(one()?)?).unwrap_or(u32::MAX)
            }
            "maxmemory-eviction-tenacity" => {
                self.maxmemory_eviction_tenacity = u32::try_from(uint(one()?)?).unwrap_or(u32::MAX)
            }
            "lfu-log-factor" => {
                self.lfu_log_factor = u32::try_from(uint(one()?)?).unwrap_or(u32::MAX)
            }
            "lfu-decay-time" => {
                self.lfu_decay_time = u32::try_from(uint(one()?)?).unwrap_or(u32::MAX)
            }

            // encodings
            "hash-max-listpack-entries" | "hash-max-ziplist-entries" => {
                self.hash_max_listpack_entries = usz(one()?)?
            }
            "hash-max-listpack-value" | "hash-max-ziplist-value" => {
                self.hash_max_listpack_value = usz(one()?)?
            }
            "list-max-listpack-size" | "list-max-ziplist-size" => {
                self.list_max_listpack_size = int(one()?)?
            }
            "list-compress-depth" => {
                self.list_compress_depth =
                    i32::try_from(int(one()?)?).map_err(|_| ConfigError::NotAnInteger)?
            }
            "set-max-intset-entries" => self.set_max_intset_entries = usz(one()?)?,
            "set-max-listpack-entries" => self.set_max_listpack_entries = usz(one()?)?,
            "set-max-listpack-value" => self.set_max_listpack_value = usz(one()?)?,
            "zset-max-listpack-entries" | "zset-max-ziplist-entries" => {
                self.zset_max_listpack_entries = usz(one()?)?
            }
            "zset-max-listpack-value" | "zset-max-ziplist-value" => {
                self.zset_max_listpack_value = usz(one()?)?
            }
            "hll-sparse-max-bytes" => self.hll_sparse_max_bytes = usz(one()?)?,
            "stream-node-max-entries" => self.stream_node_max_entries = usz(one()?)?,
            "stream-node-max-bytes" => self.stream_node_max_bytes = usz(one()?)?,

            // persistence
            "dir" => self.dir = PathBuf::from(one()?),
            "dbfilename" => self.dbfilename = one()?.to_string(),
            "save" => {
                // `save ""` clears every point; otherwise pairs accumulate.
                if args.len() == 1 && args.first().is_some_and(|s| s.is_empty()) {
                    self.save.clear();
                } else if args.len().is_multiple_of(2) && !args.is_empty() {
                    // A `save` line in a file *adds*; a CONFIG SET replaces.
                    // Redis's file parser accumulates, so we do too.
                    for pair in args.chunks_exact(2) {
                        match pair {
                            [s, c] => self.save.push(SavePoint {
                                seconds: uint(s)?,
                                changes: uint(c)?,
                            }),
                            _ => return Err(ConfigError::BadDirective),
                        }
                    }
                } else {
                    return Err(ConfigError::BadDirective);
                }
            }
            "rdbcompression" => self.rdbcompression = boolean(one()?)?,
            "rdbchecksum" => self.rdbchecksum = boolean(one()?)?,
            "stop-writes-on-bgsave-error" => self.stop_writes_on_bgsave_error = boolean(one()?)?,
            "appendonly" => self.appendonly = boolean(one()?)?,
            "appendfilename" => self.appendfilename = unquote(one()?),
            "appenddirname" => self.appenddirname = unquote(one()?),
            "appendfsync" => {
                self.appendfsync = AppendFsync::parse(one()?)
                    .ok_or_else(|| ConfigError::Invalid("invalid appendfsync".into()))?
            }
            "no-appendfsync-on-rewrite" => self.no_appendfsync_on_rewrite = boolean(one()?)?,
            "auto-aof-rewrite-percentage" => {
                self.auto_aof_rewrite_percentage = u32::try_from(uint(one()?)?).unwrap_or(u32::MAX)
            }
            "auto-aof-rewrite-min-size" => self.auto_aof_rewrite_min_size = mem(one()?)?,
            "aof-use-rdb-preamble" => self.aof_use_rdb_preamble = boolean(one()?)?,
            "aof-timestamp-enabled" => self.aof_timestamp_enabled = boolean(one()?)?,

            // security
            "requirepass" => {
                let p = one()?;
                self.requirepass = if p.is_empty() {
                    None
                } else {
                    Some(p.to_string())
                };
            }

            // notifications
            "notify-keyspace-events" => {
                self.notify_keyspace_events = NotifyClass::parse(one()?).ok_or_else(|| {
                    ConfigError::Invalid(
                        "Invalid event class character. Some possible classes are: 'g$lshzxeKE'"
                            .into(),
                    )
                })?
            }

            // introspection
            "loglevel" => self.loglevel = one()?.to_string(),
            "logfile" => self.logfile = unquote(one()?),
            "slowlog-log-slower-than" => self.slowlog_log_slower_than = int(one()?)?,
            "slowlog-max-len" => self.slowlog_max_len = usz(one()?)?,
            "latency-monitor-threshold" => self.latency_monitor_threshold = uint(one()?)?,
            "latency-tracking" => self.latency_tracking = boolean(one()?)?,

            // misc
            "activerehashing" => self.activerehashing = boolean(one()?)?,
            "activeexpire" => self.activeexpire = boolean(one()?)?,
            "lazyfree-lazy-expire" => self.lazyfree_lazy_expire = boolean(one()?)?,
            "lazyfree-lazy-eviction" => self.lazyfree_lazy_eviction = boolean(one()?)?,
            "lazyfree-lazy-server-del" => self.lazyfree_lazy_server_del = boolean(one()?)?,
            "lazyfree-lazy-user-del" => self.lazyfree_lazy_user_del = boolean(one()?)?,
            "lazyfree-lazy-user-flush" => self.lazyfree_lazy_user_flush = boolean(one()?)?,
            "io-threads" => self.io_threads = usz(one()?)?,
            "enable-protected-configs" => self.enable_protected_configs = boolean(one()?)?,
            "enable-debug-command" => self.enable_debug_command = boolean(one()?)?,
            "daemonize" => self.daemonize = boolean(one()?)?,
            "pidfile" => self.pidfile = Some(PathBuf::from(one()?)),
            "set-proc-title" => self.set_proc_title = boolean(one()?)?,

            // Accepted and ignored: directives that only make sense for
            // features that are explicitly out of scope for v1 (§1).
            "protected-mode"
            | "supervised"
            | "always-show-logo"
            | "syslog-enabled"
            | "repl-diskless-sync"
            | "repl-diskless-load"
            | "replica-read-only"
            | "replica-serve-stale-data"
            | "repl-disable-tcp-nodelay"
            | "acllog-max-len"
            | "aclfile"
            | "dynamic-hz"
            | "hz"
            | "crash-log-enabled"
            | "propagation-error-behavior"
            | "shutdown-on-sigterm"
            | "locale-collate" => {}

            other => return Err(ConfigError::Unknown(other.to_string())),
        }
        Ok(())
    }

    /// Read a redis.conf-style file.
    ///
    /// Blank lines and `#` comments are skipped; values may be double- or
    /// single-quoted; `include` is honoured.
    pub fn load_file(&mut self, path: &Path) -> Result<(), ConfigError> {
        let text = std::fs::read_to_string(path)?;
        self.load_str(&text, path.parent())
    }

    /// Parse config text. `base` is the directory `include` resolves against.
    pub fn load_str(&mut self, text: &str, base: Option<&Path>) -> Result<(), ConfigError> {
        for raw in text.lines() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let tokens = split_line(line);
            let Some((name, args)) = tokens.split_first() else {
                continue;
            };
            if name.eq_ignore_ascii_case("include") {
                if let Some(rel) = args.first() {
                    let p = match base {
                        Some(b) => b.join(rel),
                        None => PathBuf::from(rel),
                    };
                    self.load_file(&p)?;
                }
                continue;
            }
            self.apply(name, args)?;
        }
        Ok(())
    }

    /// Sanity-check values that would otherwise blow up later.
    ///
    /// Deliberately permissive: this also runs on every `CONFIG SET`, so a
    /// rule here rejects a runtime change, not just a bad config file. In
    /// particular `port 0` is *allowed* -- unlike Redis, where it means "no
    /// TCP listener", here it means "let the kernel choose", which is what the
    /// integration tests bind with.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.databases == 0 {
            return Err(ConfigError::Invalid("databases must be >= 1".into()));
        }
        if self.maxmemory_samples == 0 {
            return Err(ConfigError::Invalid(
                "maxmemory-samples must be >= 1".into(),
            ));
        }
        Ok(())
    }

    /// Effective shard count: the configured value rounded up to a power of
    /// two, or the core count rounded up when unset (§2).
    pub fn effective_shard_count(&self) -> usize {
        let base = if self.shard_count == 0 {
            num_cpus::get()
        } else {
            self.shard_count
        };
        base.max(1).next_power_of_two()
    }

    /// Bind addresses with Redis's `-` prefix (meaning "optional") stripped,
    /// and `*` expanded.
    pub fn bind_addrs(&self) -> Vec<String> {
        self.bind
            .iter()
            .map(|b| b.trim_start_matches('-').to_string())
            .map(|b| if b == "*" { "0.0.0.0".to_string() } else { b })
            .collect()
    }
}

/// A live, hot-swappable configuration.
///
/// `load()` is a `seq_cst`-free RCU read: on the command path it is a couple
/// of atomic loads and no contention, which is the whole point of using
/// `ArcSwap` here instead of an `RwLock`.
#[derive(Debug)]
pub struct ConfigHandle {
    inner: ArcSwap<Config>,
}

impl ConfigHandle {
    pub fn new(cfg: Config) -> Self {
        ConfigHandle {
            inner: ArcSwap::from(Arc::new(cfg)),
        }
    }

    /// A snapshot. Cheap enough to call per command; §5.6-style caching is
    /// still preferable inside a loop.
    #[inline]
    pub fn load(&self) -> arc_swap::Guard<Arc<Config>> {
        self.inner.load()
    }

    /// A full `Arc` clone, for holding across an await point.
    #[inline]
    pub fn snapshot(&self) -> Arc<Config> {
        self.inner.load_full()
    }

    /// Replace the configuration wholesale. `CONFIG SET` clones the current
    /// snapshot, mutates the clone and stores it, so readers never observe a
    /// half-applied change.
    pub fn store(&self, cfg: Config) {
        self.inner.store(Arc::new(cfg));
    }

    /// Read-modify-write helper for `CONFIG SET`.
    pub fn update<F>(&self, f: F) -> Result<(), ConfigError>
    where
        F: FnOnce(&mut Config) -> Result<(), ConfigError>,
    {
        let mut next = (*self.snapshot()).clone();
        f(&mut next)?;
        next.validate()?;
        self.store(next);
        Ok(())
    }
}

/// Redis's memory-size suffixes.
///
/// From `config.c:memtoull()`: `k` = 1000, `kb` = 1024, `m` = 1000*1000,
/// `mb` = 1024*1024, `g` = 1000^3, `gb` = 1024^3. The bare number is bytes.
pub fn parse_memory(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let lower = s.to_ascii_lowercase();
    let (digits, mult) = if let Some(d) = lower.strip_suffix("kb") {
        (d, 1024u64)
    } else if let Some(d) = lower.strip_suffix("mb") {
        (d, 1024 * 1024)
    } else if let Some(d) = lower.strip_suffix("gb") {
        (d, 1024 * 1024 * 1024)
    } else if let Some(d) = lower.strip_suffix('k') {
        (d, 1000)
    } else if let Some(d) = lower.strip_suffix('m') {
        (d, 1_000_000)
    } else if let Some(d) = lower.strip_suffix('g') {
        (d, 1_000_000_000)
    } else if let Some(d) = lower.strip_suffix('b') {
        (d, 1)
    } else {
        (lower.as_str(), 1)
    };
    let n: u64 = digits.trim().parse().ok()?;
    n.checked_mul(mult)
}

/// Strip one layer of matching quotes.
fn unquote(s: &str) -> String {
    let b = s.as_bytes();
    if b.len() >= 2 {
        let first = b.first().copied();
        let last = b.last().copied();
        if (first == Some(b'"') && last == Some(b'"'))
            || (first == Some(b'\'') && last == Some(b'\''))
        {
            return s.get(1..s.len() - 1).unwrap_or(s).to_string();
        }
    }
    s.to_string()
}

/// Split a config line into tokens, honouring double and single quotes.
/// Mirrors `sdssplitargs()`.
fn split_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_double = false;
    let mut in_single = false;
    let mut started = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\\' if in_double => {
                if let Some(n) = chars.next() {
                    cur.push(match n {
                        'n' => '\n',
                        'r' => '\r',
                        't' => '\t',
                        other => other,
                    });
                }
            }
            '"' if !in_single => {
                in_double = !in_double;
                started = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
                started = true;
            }
            c if c.is_whitespace() && !in_double && !in_single => {
                if started || !cur.is_empty() {
                    out.push(core::mem::take(&mut cur));
                    started = false;
                }
            }
            c => {
                cur.push(c);
                started = true;
            }
        }
    }
    if started || !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Command-line interface, matching `redis-server`'s shape:
///
/// ```text
/// rsdis [/path/to/redis.conf] [--directive value ...]
/// ```
///
/// The derive exists for `--help` and `--version` only. clap cannot model
/// this grammar directly: in `rsdis --port 6380` there is no positional before
/// the first `--flag`, so clap rejects `--port` as an unknown argument, and
/// `trailing_var_arg` does not engage until a positional has been consumed.
/// [`Cli::from_env`] therefore splits argv the way `redis-server` does and
/// only defers to clap for the two informational flags.
#[derive(Debug, Parser)]
#[command(name = "rsdis", version, about = "A Redis-compatible server", long_about = None)]
pub struct Cli {
    /// Path to a redis.conf-style configuration file.
    pub config_file: Option<PathBuf>,

    /// Configuration overrides, exactly as redis-server accepts them:
    /// `--port 6380 --maxmemory 100mb --save ""`.
    #[arg(
        trailing_var_arg = true,
        allow_hyphen_values = true,
        value_name = "OVERRIDES"
    )]
    pub overrides: Vec<String>,
}

impl Cli {
    /// Parse the real process arguments.
    pub fn from_env() -> Self {
        Cli::from_args(std::env::args().skip(1).collect::<Vec<_>>())
    }

    /// Split a redis-server-style argument list.
    ///
    /// The first argument is the config file if and only if it does not start
    /// with `-`; everything after it is a directive stream.
    pub fn from_args(args: Vec<String>) -> Self {
        use clap::CommandFactory;

        if args.iter().any(|a| a == "--help" || a == "-h") {
            let mut cmd = Cli::command();
            let _ = cmd.print_help();
            println!();
            std::process::exit(0);
        }
        if args.iter().any(|a| a == "--version" || a == "-V") {
            println!("rsdis {}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }

        let takes_config_file = args.first().is_some_and(|a| !a.starts_with('-'));
        let (config_file, overrides) = if takes_config_file {
            (
                args.first().map(PathBuf::from),
                args.get(1..).unwrap_or(&[]).to_vec(),
            )
        } else {
            (None, args)
        };
        Cli {
            config_file,
            overrides,
        }
    }

    /// Build a `Config` from the file (if any) plus the overrides.
    pub fn into_config(self) -> Result<Config, ConfigError> {
        let mut cfg = Config::default();
        if let Some(path) = &self.config_file {
            cfg.load_file(path)?;
        }
        apply_overrides(&mut cfg, &self.overrides)?;
        cfg.validate()?;
        Ok(cfg)
    }
}

/// Fold `["--port", "6380", "--save", ""]` into directives.
///
/// A CLI `--save ""` replaces the accumulated save points rather than adding
/// to them, which is what `redis-server --save ""` does.
pub fn apply_overrides(cfg: &mut Config, tokens: &[String]) -> Result<(), ConfigError> {
    let mut i = 0usize;
    while i < tokens.len() {
        let Some(tok) = tokens.get(i) else { break };
        let Some(name) = tok.strip_prefix("--") else {
            return Err(ConfigError::Invalid(format!(
                "Bad directive or wrong number of arguments: {tok}"
            )));
        };
        let mut args: Vec<String> = Vec::new();
        i += 1;
        while let Some(next) = tokens.get(i) {
            if next.starts_with("--") && next.len() > 2 && !is_negative_number(next) {
                break;
            }
            args.push(next.clone());
            i += 1;
        }
        if name.eq_ignore_ascii_case("save") {
            cfg.save.clear();
            if args.len() == 1 && args.first().is_some_and(|s| s.is_empty()) {
                continue;
            }
        }
        cfg.apply(name, &args)?;
    }
    Ok(())
}

fn is_negative_number(s: &str) -> bool {
    s.strip_prefix("--")
        .is_some_and(|r| r.parse::<i64>().is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_redis() {
        let c = Config::default();
        assert_eq!(c.port, 6379);
        assert_eq!(c.databases, 16);
        assert_eq!(c.maxmemory, 0);
        assert_eq!(c.maxmemory_policy, MaxmemoryPolicy::NoEviction);
        assert_eq!(c.hash_max_listpack_entries, 128);
        assert_eq!(c.hash_max_listpack_value, 64);
        assert_eq!(c.list_max_listpack_size, 128);
        assert_eq!(c.set_max_intset_entries, 512);
        assert_eq!(c.set_max_listpack_entries, 128);
        assert_eq!(c.zset_max_listpack_entries, 128);
        assert_eq!(c.zset_max_listpack_value, 64);
        assert!(!c.appendonly);
        assert_eq!(c.appendfsync, AppendFsync::EverySec);
        assert_eq!(c.timeout, 0);
        assert_eq!(c.tcp_keepalive, 300);
        assert!(c.requirepass.is_none());
        assert_eq!(c.notify_keyspace_events, NotifyClass::empty());
        assert_eq!(c.save.len(), 3);
        assert_eq!(c.proto_max_bulk_len, 512 * 1024 * 1024);
        assert_eq!(c.slowlog_log_slower_than, 10_000);
        assert_eq!(c.slowlog_max_len, 128);
    }

    #[test]
    fn memory_suffixes() {
        assert_eq!(parse_memory("100"), Some(100));
        assert_eq!(parse_memory("1k"), Some(1000));
        assert_eq!(parse_memory("1kb"), Some(1024));
        assert_eq!(parse_memory("1m"), Some(1_000_000));
        assert_eq!(parse_memory("1mb"), Some(1024 * 1024));
        assert_eq!(parse_memory("1g"), Some(1_000_000_000));
        assert_eq!(parse_memory("1gb"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory("100MB"), Some(100 * 1024 * 1024));
        assert_eq!(parse_memory("0"), Some(0));
        assert_eq!(parse_memory("abc"), None);
        assert_eq!(parse_memory(""), None);
    }

    #[test]
    fn parses_a_redis_conf() {
        let text = r#"
# a comment
port 6380
bind 127.0.0.1 ::1
maxmemory 100mb
maxmemory-policy allkeys-lru
appendonly yes
appendfsync always
save 900 1
save 300 10
requirepass "hunter2"
notify-keyspace-events "KEA"
hash-max-listpack-entries 256
logfile ""
"#;
        let mut c = Config::default();
        c.load_str(text, None).expect("parse");
        assert_eq!(c.port, 6380);
        assert_eq!(c.bind, vec!["127.0.0.1", "::1"]);
        assert_eq!(c.maxmemory, 100 * 1024 * 1024);
        assert_eq!(c.maxmemory_policy, MaxmemoryPolicy::AllkeysLru);
        assert!(c.appendonly);
        assert_eq!(c.appendfsync, AppendFsync::Always);
        assert_eq!(c.requirepass.as_deref(), Some("hunter2"));
        assert_eq!(c.hash_max_listpack_entries, 256);
        assert_eq!(c.logfile, "");
        assert!(c.notify_keyspace_events.contains(NotifyClass::KEYSPACE));
        // The three defaults plus the two configured.
        assert_eq!(c.save.len(), 5);
    }

    #[test]
    fn save_empty_string_clears() {
        let mut c = Config::default();
        c.load_str("save \"\"", None).unwrap();
        assert!(c.save.is_empty());
    }

    #[test]
    fn unknown_directive_is_an_error() {
        let mut c = Config::default();
        let e = c
            .load_str("maxmemory-polciy allkeys-lru", None)
            .unwrap_err();
        assert!(matches!(e, ConfigError::Unknown(_)), "{e}");
    }

    #[test]
    fn bad_values_are_errors() {
        let mut c = Config::default();
        assert!(c.apply("port", &["abc".into()]).is_err());
        assert!(c.apply("appendonly", &["maybe".into()]).is_err());
        assert!(c.apply("maxmemory-policy", &["nonsense".into()]).is_err());
        assert!(c.apply("databases", &["0".into()]).is_err());
        assert!(c.apply("port", &[]).is_err());
    }

    #[test]
    fn cli_overrides() {
        let cli = Cli {
            config_file: None,
            overrides: vec![
                "--port".into(),
                "7000".into(),
                "--maxmemory".into(),
                "64mb".into(),
                "--save".into(),
                String::new(),
                "--appendonly".into(),
                "yes".into(),
            ],
        };
        let c = cli.into_config().expect("config");
        assert_eq!(c.port, 7000);
        assert_eq!(c.maxmemory, 64 * 1024 * 1024);
        assert!(c.save.is_empty());
        assert!(c.appendonly);
    }

    #[test]
    fn cli_splits_redis_server_style_argv() {
        // Flags with no config file -- the case clap alone cannot parse.
        let cli = Cli::from_args(vec!["--port".into(), "6399".into()]);
        assert_eq!(cli.config_file, None);
        assert_eq!(cli.overrides, vec!["--port", "6399"]);
        assert_eq!(cli.into_config().unwrap().port, 6399);

        // Config file followed by overrides.
        let cli = Cli::from_args(vec![
            "/etc/redis.conf".into(),
            "--port".into(),
            "7000".into(),
        ]);
        assert_eq!(cli.config_file, Some(PathBuf::from("/etc/redis.conf")));
        assert_eq!(cli.overrides, vec!["--port", "7000"]);

        // Nothing at all.
        let cli = Cli::from_args(vec![]);
        assert_eq!(cli.config_file, None);
        assert!(cli.overrides.is_empty());
        assert_eq!(cli.into_config().unwrap().port, 6379);
    }

    #[test]
    fn cli_multi_value_override() {
        let mut c = Config::default();
        apply_overrides(
            &mut c,
            &["--bind".into(), "127.0.0.1".into(), "10.0.0.1".into()],
        )
        .unwrap();
        assert_eq!(c.bind, vec!["127.0.0.1", "10.0.0.1"]);
    }

    #[test]
    fn cli_negative_values_are_not_flags() {
        let mut c = Config::default();
        apply_overrides(&mut c, &["--list-compress-depth".into(), "-1".into()]).unwrap();
        assert_eq!(c.list_compress_depth, -1);
    }

    #[test]
    fn shard_count_derives_and_rounds() {
        let mut c = Config {
            shard_count: 10,
            ..Default::default()
        };
        assert_eq!(c.effective_shard_count(), 16);
        c.shard_count = 16;
        assert_eq!(c.effective_shard_count(), 16);
        c.shard_count = 0;
        assert!(c.effective_shard_count().is_power_of_two());
    }

    #[test]
    fn bind_addrs_normalises() {
        let c = Config {
            bind: vec!["-::1".into(), "*".into(), "127.0.0.1".into()],
            ..Default::default()
        };
        assert_eq!(c.bind_addrs(), vec!["::1", "0.0.0.0", "127.0.0.1"]);
    }

    #[test]
    fn handle_is_copy_on_write() {
        let h = ConfigHandle::new(Config::default());
        let before = h.snapshot();
        h.update(|c| {
            c.maxmemory = 42;
            Ok(())
        })
        .unwrap();
        // The old snapshot is untouched: an in-flight command keeps its view.
        assert_eq!(before.maxmemory, 0);
        assert_eq!(h.load().maxmemory, 42);
    }

    #[test]
    fn handle_rejects_invalid_updates() {
        let h = ConfigHandle::new(Config::default());
        assert!(
            h.update(|c| {
                c.databases = 0;
                Ok(())
            })
            .is_err()
        );
        assert_eq!(h.load().databases, 16);
    }

    #[test]
    fn split_line_handles_quotes() {
        assert_eq!(split_line("a b c"), vec!["a", "b", "c"]);
        assert_eq!(split_line(r#"a "b c" d"#), vec!["a", "b c", "d"]);
        assert_eq!(split_line(r#"save """#), vec!["save", ""]);
        assert_eq!(split_line("a 'b c'"), vec!["a", "b c"]);
        assert_eq!(split_line(r#"a "b\nc""#), vec!["a", "b\nc"]);
    }

    #[test]
    fn include_is_followed() {
        let dir = tempfile::tempdir().unwrap();
        let inner = dir.path().join("inner.conf");
        std::fs::write(&inner, "port 6399\n").unwrap();
        let outer = dir.path().join("outer.conf");
        std::fs::write(&outer, "include inner.conf\nmaxmemory 1mb\n").unwrap();
        let mut c = Config::default();
        c.load_file(&outer).unwrap();
        assert_eq!(c.port, 6399);
        assert_eq!(c.maxmemory, 1024 * 1024);
    }
}
