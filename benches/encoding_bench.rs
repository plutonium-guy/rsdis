//! Benchmarks for the W1a encodings.
//!
//! Covers the operations W2 leans on hardest: listpack append/get/iterate,
//! intset add/contains, quicklist push/index, skiplist insert/rank/range, and
//! rax insert/seek. Sizes are the ones the server actually runs at -- 128
//! entries for a listpack (`hash-max-listpack-entries`), 512 for an intset
//! (`set-max-intset-entries`), and 1k/100k for the structures that only exist
//! above those thresholds.
//!
//! # Running
//!
//! ```text
//! cargo test --release --bench encoding_bench -- --ignored --nocapture
//! ```
//!
//! They are `#[ignore]`d so a plain `cargo test` stays fast.
//!
//! # Why not criterion
//!
//! `Cargo.toml` is F0-owned and has no
//!
//! ```toml
//! [[bench]]
//! name = "encoding_bench"
//! harness = false
//! ```
//!
//! target. Without it Cargo's bench autodiscovery builds this file against the
//! libtest harness, so a criterion `main` would never be called (`cargo bench`
//! reports "running 0 tests") and would trip `clippy --all-targets -D
//! warnings` as dead code. W1b and W1c hit the same wall. This file therefore
//! self-times against `std::time::Instant`; it is a contract gap to fix at
//! merge, not something to work around by editing a file this agent does not
//! own. Numbers below are medians of repeated batches, which is enough to
//! rank implementations but is not criterion's statistics.

use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rsdis::encoding::intset::Intset;
use rsdis::encoding::listpack::{Listpack, ListpackEntry};
use rsdis::encoding::quicklist::Quicklist;
use rsdis::encoding::rax::{Rax, Seek};
use rsdis::encoding::skiplist::{ScoreBound, ScoreRange, Skiplist};

// ---------------------------------------------------------------------------
// timing harness
// ---------------------------------------------------------------------------

/// Target wall time per measured batch. Long enough to swamp timer noise,
/// short enough that the whole suite stays under a minute.
const BATCH: Duration = Duration::from_millis(120);
/// Batches measured; the median is reported, so a stray scheduling hiccup
/// cannot dominate.
const ROUNDS: usize = 5;

/// Time `f`, which performs `per_iter` logical operations per call.
///
/// Auto-scales the iteration count until a batch takes about [`BATCH`], then
/// reports the median of [`ROUNDS`] batches as ns per operation.
fn bench(name: &str, per_iter: u64, mut f: impl FnMut()) {
    // Warm up and calibrate.
    let mut iters: u64 = 1;
    loop {
        let t = Instant::now();
        for _ in 0..iters {
            f();
        }
        let dt = t.elapsed();
        if dt >= BATCH || iters >= 1 << 30 {
            break;
        }
        // Scale toward BATCH, capped so a fast op does not overshoot wildly.
        let factor = (BATCH.as_nanos() / dt.as_nanos().max(1)).clamp(2, 64) as u64;
        iters = iters.saturating_mul(factor);
    }

    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let t = Instant::now();
        for _ in 0..iters {
            f();
        }
        samples.push(t.elapsed().as_nanos() as f64 / (iters * per_iter) as f64);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let ns = samples[samples.len() / 2];

    let rate = if ns > 0.0 { 1_000.0 / ns } else { f64::NAN };
    println!("  {name:<42} {ns:>12.2} ns/op  {rate:>10.1} Mop/s");
}

/// Time `f` over a state built fresh by `setup`, so teardown and construction
/// are outside the measured region.
fn bench_with<S>(
    name: &str,
    per_iter: u64,
    mut setup: impl FnMut() -> S,
    mut f: impl FnMut(&mut S),
) {
    let mut samples = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let mut s = setup();
        let t = Instant::now();
        f(&mut s);
        samples.push(t.elapsed().as_nanos() as f64 / per_iter as f64);
        drop(s);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let ns = samples[samples.len() / 2];
    let rate = if ns > 0.0 { 1_000.0 / ns } else { f64::NAN };
    println!("  {name:<42} {ns:>12.2} ns/op  {rate:>10.1} Mop/s");
}

fn header(title: &str) {
    println!("\n=== {title} ===");
}

// ---------------------------------------------------------------------------
// listpack
// ---------------------------------------------------------------------------

fn listpack_values(n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("field:{i:06}").into_bytes())
        .collect()
}

fn listpack_of(n: usize) -> Listpack {
    let vals = listpack_values(n);
    Listpack::from_entries(
        vals.iter()
            .map(|v| ListpackEntry::Str(v))
            .collect::<Vec<_>>(),
    )
}

#[test]
#[ignore = "benchmark"]
fn bench_listpack() {
    header("listpack");

    // Append: the hash/zset build path.
    for n in [16usize, 128, 512] {
        let vals = listpack_values(n);
        bench(&format!("append/{n}"), n as u64, || {
            let mut lp = Listpack::with_capacity(vals.len() * 16);
            for v in &vals {
                lp.append(ListpackEntry::Str(v));
            }
            black_box(lp.total_bytes());
        });
    }

    // Random get: HGET / LINDEX / ZSCORE on a listpack-encoded object.
    for n in [16usize, 128] {
        let lp = listpack_of(n);
        let mid = (n / 2) as isize;
        bench(&format!("get_middle/{n}"), 1, || {
            black_box(lp.get(black_box(mid)).is_some());
        });
        bench(&format!("get_last/{n}"), 1, || {
            black_box(lp.get(black_box(-1)).is_some());
        });
    }

    // Full iteration: HGETALL / LRANGE.
    for n in [128usize, 512] {
        let lp = listpack_of(n);
        bench(&format!("iterate/{n}"), n as u64, || {
            let mut acc = 0usize;
            for e in lp.iter() {
                acc += e.byte_len();
            }
            black_box(acc);
        });
        bench(&format!("iterate_rev/{n}"), n as u64, || {
            let mut acc = 0usize;
            for e in lp.iter_rev() {
                acc += e.byte_len();
            }
            black_box(acc);
        });
    }

    // Field lookup with the stride a hash uses (skip over the values).
    let n = 128usize;
    let mut hash = Listpack::new();
    for i in 0..n {
        hash.append(ListpackEntry::Str(format!("f{i:04}").as_bytes()));
        hash.append(ListpackEntry::Str(format!("v{i:04}").as_bytes()));
    }
    let needle = format!("f{:04}", n - 1).into_bytes();
    bench("find_field_stride2/128 (worst case)", 1, || {
        black_box(hash.find_from(0, black_box(&needle), 1));
    });

    // Validation, which W3a runs on every listpack loaded from an RDB.
    let lp = listpack_of(128);
    bench("validate/128", 128, || {
        black_box(Listpack::validate(black_box(lp.as_bytes())));
    });
}

// ---------------------------------------------------------------------------
// intset
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark"]
fn bench_intset() {
    header("intset");

    for n in [64usize, 512] {
        // Ascending insert hits intsetSearch's out-of-range short circuit.
        bench(&format!("add_ascending/{n}"), n as u64, || {
            let mut s = Intset::new();
            for i in 0..n as i64 {
                s.add(i);
            }
            black_box(s.len());
        });
        // Scattered insert pays the full binary search plus a memmove.
        let scattered: Vec<i64> = (0..n as i64)
            .map(|i| i.wrapping_mul(2_654_435_761) % 1_000_003)
            .collect();
        bench(&format!("add_scattered/{n}"), n as u64, || {
            let mut s = Intset::new();
            for x in &scattered {
                s.add(*x);
            }
            black_box(s.len());
        });
    }

    for n in [64usize, 512, 4096] {
        let s = Intset::from_iter_i64((0..n as i64).map(|i| i * 3));
        let probe = (n as i64 / 2) * 3;
        bench(&format!("contains_hit/{n}"), 1, || {
            black_box(s.contains(black_box(probe)));
        });
        bench(&format!("contains_miss/{n}"), 1, || {
            black_box(s.contains(black_box(1)));
        });
    }

    // The upgrade path: a 16-bit set forced all the way to 64-bit.
    bench_with(
        "upgrade_16_to_64/512",
        512,
        || Intset::from_iter_i64(0..512),
        |s| {
            s.add(i64::MAX);
            black_box(s.encoding());
        },
    );
}

// ---------------------------------------------------------------------------
// quicklist
// ---------------------------------------------------------------------------

#[test]
#[ignore = "benchmark"]
fn bench_quicklist() {
    header("quicklist");
    let val: &[u8] = b"a-typical-list-element-value";

    // RPUSH / LPUSH at both defaults: count fill (128) and size fill (-2, 8 KB).
    for fill in [128i64, -2] {
        let name = if fill >= 0 { "count" } else { "size" };
        for n in [1_000usize, 100_000] {
            bench(&format!("push_tail_{name}/{n}"), n as u64, || {
                let mut ql = Quicklist::new(fill);
                for _ in 0..n {
                    ql.push_tail(val);
                }
                black_box(ql.len());
            });
            bench(&format!("push_head_{name}/{n}"), n as u64, || {
                let mut ql = Quicklist::new(fill);
                for _ in 0..n {
                    ql.push_head(val);
                }
                black_box(ql.len());
            });
        }
    }

    // LINDEX: the ends are O(1)-ish, the middle is the node walk.
    for n in [1_000usize, 100_000] {
        let ql = Quicklist::from_values(-2, (0..n).map(|i| format!("v{i:07}")));
        let mid = (n / 2) as i64;
        bench(&format!("index_head/{n}"), 1, || {
            black_box(ql.index(black_box(0)).is_some());
        });
        bench(&format!("index_tail/{n}"), 1, || {
            black_box(ql.index(black_box(-1)).is_some());
        });
        bench(&format!("index_middle/{n}"), 1, || {
            black_box(ql.index(black_box(mid)).is_some());
        });
        bench(&format!("iterate/{n}"), n as u64, || {
            let mut acc = 0usize;
            for e in ql.iter() {
                acc += e.byte_len();
            }
            black_box(acc);
        });
    }

    // LPOP drain.
    bench_with(
        "pop_head_drain/10k",
        10_000,
        || Quicklist::from_values(-2, (0..10_000).map(|i| format!("v{i:07}"))),
        |ql| {
            while ql.drop_head() {}
            black_box(ql.len());
        },
    );
}

// ---------------------------------------------------------------------------
// skiplist
// ---------------------------------------------------------------------------

/// Members are pre-built so the benchmark times the skiplist, not `format!`
/// and not the allocator. `Bytes::clone` is a refcount bump, which is exactly
/// what W2c's zset will do when it shares a member with its dict.
fn skiplist_members(n: usize) -> Vec<(f64, Bytes)> {
    (0..n)
        .map(|i| {
            // Scattered scores, so the level distribution is realistic.
            let s = ((i as u64).wrapping_mul(2_654_435_761) % 1_000_000) as f64;
            (s, Bytes::from(format!("member:{i:07}")))
        })
        .collect()
}

fn skiplist_of(members: &[(f64, Bytes)]) -> Skiplist {
    let mut sl = Skiplist::new();
    for (s, m) in members {
        sl.insert(*s, m.clone());
    }
    sl
}

#[test]
#[ignore = "benchmark"]
fn bench_skiplist() {
    header("skiplist");

    for n in [1_000usize, 100_000] {
        let members = skiplist_members(n);
        bench(&format!("insert/{n}"), n as u64, || {
            black_box(skiplist_of(&members).len());
        });
    }

    for n in [1_000usize, 100_000] {
        let members = skiplist_members(n);
        let sl = skiplist_of(&members);
        let probe_rank = n / 2;
        let (member, score) = sl
            .by_rank(probe_rank)
            .map(|(m, s)| (m.clone(), s))
            .expect("rank in range");

        bench(&format!("rank_of/{n}"), 1, || {
            black_box(sl.rank_of(black_box(score), black_box(&member)));
        });
        bench(&format!("by_rank/{n}"), 1, || {
            black_box(sl.by_rank(black_box(probe_rank)).is_some());
        });

        // ZRANGEBYSCORE over roughly 1% of the set.
        let r = ScoreRange::new(ScoreBound::incl(0.0), ScoreBound::incl(10_000.0));
        let hits = sl.range(&r).count().max(1) as u64;
        bench(&format!("range_by_score/{n} ({hits} hits)"), hits, || {
            let mut acc = 0usize;
            for (m, _) in sl.range(black_box(&r)) {
                acc += m.len();
            }
            black_box(acc);
        });

        // ZRANGE by rank, 100 elements from the middle.
        bench(&format!("range_by_rank_100/{n}"), 100, || {
            let mut acc = 0usize;
            for (m, _) in sl.iter_from_rank(black_box(probe_rank)).take(100) {
                acc += m.len();
            }
            black_box(acc);
        });

        // Reordering update: the delete + reinsert path.
        let mut work = sl.clone();
        let mut flip = false;
        bench(&format!("update_score_reorder/{n}"), 1, || {
            let (from, to) = if flip { (1e9, score) } else { (score, 1e9) };
            work.update_score(from, &member, to);
            flip = !flip;
        });
    }
}

// ---------------------------------------------------------------------------
// rax
// ---------------------------------------------------------------------------

/// 128-bit big-endian stream IDs, the shape `t_stream.c` uses.
fn stream_ids(n: usize) -> Vec<[u8; 16]> {
    (0..n as u64)
        .map(|i| {
            let mut b = [0u8; 16];
            b[..8].copy_from_slice(&(1_700_000_000_000u64 + i / 4).to_be_bytes());
            b[8..].copy_from_slice(&(i % 4).to_be_bytes());
            b
        })
        .collect()
}

fn rax_of(ids: &[[u8; 16]]) -> Rax<u32> {
    let mut r: Rax<u32> = Rax::new();
    for (i, k) in ids.iter().enumerate() {
        r.insert(k, i as u32);
    }
    r
}

#[test]
#[ignore = "benchmark"]
fn bench_rax() {
    header("rax");

    for n in [1_000usize, 100_000] {
        let ids = stream_ids(n);
        bench(&format!("insert_stream_ids/{n}"), n as u64, || {
            black_box(rax_of(&ids).len());
        });
    }

    for n in [1_000usize, 100_000] {
        let ids = stream_ids(n);
        let r = rax_of(&ids);
        let mid = ids[n / 2];

        bench(&format!("find/{n}"), 1, || {
            black_box(r.find(black_box(&mid)).is_some());
        });
        bench(&format!("seek_ge/{n}"), 1, || {
            let mut it = r.seek(Seek::Ge, black_box(&mid));
            black_box(it.next().is_some());
        });
        bench(&format!("seek_le/{n}"), 1, || {
            let mut it = r.seek_rev(Seek::Le, black_box(&mid));
            black_box(it.next().is_some());
        });
        // XRANGE of 100 entries starting mid-stream.
        bench(&format!("seek_then_scan_100/{n}"), 100, || {
            let mut it = r.seek(Seek::Ge, black_box(&mid));
            let mut acc = 0usize;
            for _ in 0..100 {
                match it.next() {
                    Some((k, _)) => acc += k.len(),
                    None => break,
                }
            }
            black_box(acc);
        });
        bench(&format!("iterate_all/{n}"), n as u64, || {
            let mut it = r.iter();
            let mut acc = 0usize;
            while let Some((k, _)) = it.next() {
                acc += k.len();
            }
            black_box(acc);
        });
    }

    // Removal, which exercises the prune + re-compress path.
    let ids = stream_ids(10_000);
    bench_with(
        "remove_drain/10k",
        10_000,
        || rax_of(&ids),
        |r| {
            for k in &ids {
                r.remove(k);
            }
            black_box(r.len());
        },
    );
}
