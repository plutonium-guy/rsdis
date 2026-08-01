//! Differential tests for the W1a encodings.
//!
//! Every structure here is one where an off-by-one silently corrupts data
//! rather than crashing, so each gets a `proptest` that mirrors a random
//! operation sequence against a naive model -- `Vec` for the sequences,
//! `BTreeSet`/`BTreeMap` for the ordered ones -- and asserts they agree after
//! every step, not just at the end.
//!
//! The fixed-input tests alongside them pin the cases a random generator is
//! unlikely to hit often enough: the encoding-width boundaries (127/128,
//! 4095/4096, the i16/i32/i64 limits), the empty and single-element states,
//! deleting the last element, and reverse iteration after a deletion.

use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;
use proptest::prelude::*;
use rsdis::encoding::intset::Intset;
use rsdis::encoding::listpack::{Listpack, ListpackEntry};
use rsdis::encoding::quicklist::Quicklist;
use rsdis::encoding::rax::{Rax, Seek};
use rsdis::encoding::skiplist::{LexBound, LexRange, ScoreBound, ScoreRange, Skiplist};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// The textual form of a listpack entry, which is what a Redis client sees.
fn text(e: ListpackEntry<'_>) -> Vec<u8> {
    e.to_buf().to_vec()
}

fn lp_dump(lp: &Listpack) -> Vec<Vec<u8>> {
    lp.iter().map(text).collect()
}

fn lp_dump_rev(lp: &Listpack) -> Vec<Vec<u8>> {
    let mut v: Vec<Vec<u8>> = lp.iter_rev().map(text).collect();
    v.reverse();
    v
}

fn ql_dump(ql: &Quicklist) -> Vec<Vec<u8>> {
    ql.iter().map(text).collect()
}

fn ql_dump_rev(ql: &Quicklist) -> Vec<Vec<u8>> {
    let mut v: Vec<Vec<u8>> = ql.iter_rev().map(text).collect();
    v.reverse();
    v
}

fn sl_dump(sl: &Skiplist) -> Vec<(f64, Vec<u8>)> {
    sl.iter().map(|(m, s)| (s, m.to_vec())).collect()
}

fn rax_dump<V: Clone>(r: &Rax<V>) -> Vec<(Vec<u8>, V)> {
    let mut out = Vec::new();
    let mut it = r.iter();
    while let Some((k, v)) = it.next() {
        out.push((k.to_vec(), v.clone()));
    }
    out
}

// ---------------------------------------------------------------------------
// listpack
// ---------------------------------------------------------------------------

/// Values chosen to straddle every encoding boundary in the ladder.
fn lp_value() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Integers around each width boundary, as decimal text.
        prop_oneof![
            Just(0i64),
            Just(127),
            Just(128),
            Just(-1),
            Just(4095),
            Just(4096),
            Just(-4096),
            Just(-4097),
            Just(i64::from(i16::MAX)),
            Just(i64::from(i16::MAX) + 1),
            Just(i64::from(i16::MIN)),
            Just(i64::from(i16::MIN) - 1),
            Just(8_388_607),
            Just(8_388_608),
            Just(i64::from(i32::MAX)),
            Just(i64::from(i32::MAX) + 1),
            Just(i64::MIN),
            Just(i64::MAX),
            any::<i64>(),
        ]
        .prop_map(|v| v.to_string().into_bytes()),
        // Strings around the 6/12/32-bit length boundaries.
        prop_oneof![
            Just(0usize),
            Just(1),
            Just(63),
            Just(64),
            Just(4095),
            Just(4096),
        ]
        .prop_map(|n| vec![b'x'; n]),
        // Arbitrary short bytes, including things that look almost numeric.
        proptest::collection::vec(any::<u8>(), 0..24),
        "[0-9-]{0,6}".prop_map(String::into_bytes),
    ]
}

#[derive(Debug, Clone)]
enum LpOp {
    Append(Vec<u8>),
    Prepend(Vec<u8>),
    Insert(usize, Vec<u8>),
    Replace(usize, Vec<u8>),
    Delete(usize),
    DeleteRange(usize, usize),
}

fn lp_op() -> impl Strategy<Value = LpOp> {
    prop_oneof![
        4 => lp_value().prop_map(LpOp::Append),
        2 => lp_value().prop_map(LpOp::Prepend),
        2 => (0usize..40, lp_value()).prop_map(|(i, v)| LpOp::Insert(i, v)),
        2 => (0usize..40, lp_value()).prop_map(|(i, v)| LpOp::Replace(i, v)),
        2 => (0usize..40).prop_map(LpOp::Delete),
        1 => (0usize..40, 0usize..8).prop_map(|(i, n)| LpOp::DeleteRange(i, n)),
    ]
}

/// The model's view of what a value reads back as: listpack int-encodes any
/// string that `string2ll` accepts, so `"123"` comes back as `123`. That is a
/// no-op on the text, which is why the model can stay a plain `Vec<Vec<u8>>`.
fn canon(v: &[u8]) -> Vec<u8> {
    v.to_vec()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn prop_listpack_matches_vec_model(ops in proptest::collection::vec(lp_op(), 1..60)) {
        let mut lp = Listpack::new();
        let mut model: Vec<Vec<u8>> = Vec::new();

        for op in ops {
            match op {
                LpOp::Append(v) => {
                    prop_assert!(lp.append(ListpackEntry::Str(&v)));
                    model.push(canon(&v));
                }
                LpOp::Prepend(v) => {
                    prop_assert!(lp.prepend(ListpackEntry::Str(&v)));
                    model.insert(0, canon(&v));
                }
                LpOp::Insert(i, v) => {
                    let i = i.min(model.len());
                    prop_assert!(lp.insert(i, ListpackEntry::Str(&v)));
                    model.insert(i, canon(&v));
                }
                LpOp::Replace(i, v) => {
                    let ok = lp.replace(i, ListpackEntry::Str(&v));
                    prop_assert_eq!(ok, i < model.len());
                    if ok {
                        model[i] = canon(&v);
                    }
                }
                LpOp::Delete(i) => {
                    let ok = lp.delete(i as isize);
                    prop_assert_eq!(ok, i < model.len());
                    if ok {
                        model.remove(i);
                    }
                }
                LpOp::DeleteRange(i, n) => {
                    let got = lp.delete_range(i, n);
                    let want = if i >= model.len() { 0 } else { n.min(model.len() - i) };
                    prop_assert_eq!(got, want);
                    model.drain(i.min(model.len())..(i + want).min(model.len()));
                }
            }

            // Structural invariants after *every* operation.
            prop_assert!(Listpack::validate(lp.as_bytes()), "listpack corrupted");
            prop_assert_eq!(lp.len(), model.len());
            prop_assert_eq!(lp.is_empty(), model.is_empty());
            prop_assert_eq!(lp.total_bytes(), lp.as_bytes().len());
            prop_assert_eq!(lp_dump(&lp), model.clone());
            // Reverse traversal must reproduce forward traversal exactly --
            // this is what the backlen bytes exist for.
            prop_assert_eq!(lp_dump_rev(&lp), model.clone());
            prop_assert_eq!(lp.first().map(text), model.first().cloned());
            prop_assert_eq!(lp.last().map(text), model.last().cloned());
        }

        // Random access, forwards and backwards, against the model.
        for (i, want) in model.iter().enumerate() {
            prop_assert_eq!(lp.get(i as isize).map(text), Some(want.clone()));
            let neg = i as isize - model.len() as isize;
            prop_assert_eq!(lp.get(neg).map(text), Some(want.clone()));
        }
        prop_assert!(lp.get(model.len() as isize).is_none());
        prop_assert!(lp.get(-(model.len() as isize) - 1).is_none());
    }

    #[test]
    fn prop_listpack_round_trips_through_bytes(vals in proptest::collection::vec(lp_value(), 0..40)) {
        let lp = Listpack::from_entries(vals.iter().map(|v| ListpackEntry::Str(v)));
        let bytes = lp.as_bytes().to_vec();
        let back = Listpack::from_bytes(bytes).expect("our own output must validate");
        prop_assert_eq!(lp_dump(&lp), lp_dump(&back));
        prop_assert_eq!(lp.len(), back.len());
        prop_assert_eq!(lp.as_bytes(), back.as_bytes());
    }

    #[test]
    fn prop_listpack_find_agrees_with_scan(vals in proptest::collection::vec(lp_value(), 0..30),
                                           needle in lp_value()) {
        let lp = Listpack::from_entries(vals.iter().map(|v| ListpackEntry::Str(v)));
        let want = lp.iter().position(|e| e.eq_bytes(&needle));
        prop_assert_eq!(lp.find(&needle), want);
    }
}

// ---------------------------------------------------------------------------
// intset
// ---------------------------------------------------------------------------

fn int_value() -> impl Strategy<Value = i64> {
    prop_oneof![
        // Cluster around the width boundaries so upgrades actually fire.
        Just(0i64),
        Just(i64::from(i16::MAX)),
        Just(i64::from(i16::MAX) + 1),
        Just(i64::from(i16::MIN)),
        Just(i64::from(i16::MIN) - 1),
        Just(i64::from(i32::MAX)),
        Just(i64::from(i32::MAX) + 1),
        Just(i64::from(i32::MIN)),
        Just(i64::from(i32::MIN) - 1),
        Just(i64::MAX),
        Just(i64::MIN),
        -300i64..300,
        any::<i64>(),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn prop_intset_matches_btreeset_model(
        ops in proptest::collection::vec((any::<bool>(), int_value()), 1..120)
    ) {
        let mut s = Intset::new();
        let mut model: BTreeSet<i64> = BTreeSet::new();

        for (add, v) in ops {
            if add {
                prop_assert_eq!(s.add(v), model.insert(v));
            } else {
                prop_assert_eq!(s.remove(v), model.remove(&v));
            }

            prop_assert_eq!(s.len(), model.len());
            prop_assert_eq!(s.is_empty(), model.is_empty());
            prop_assert_eq!(s.iter().collect::<Vec<_>>(), model.iter().copied().collect::<Vec<_>>());
            prop_assert_eq!(s.min(), model.iter().next().copied());
            prop_assert_eq!(s.max(), model.iter().next_back().copied());
            // The blob must always be self-consistent and re-adoptable.
            prop_assert_eq!(s.as_bytes().len(), 8 + model.len() * s.encoding() as usize);
            prop_assert!(Intset::from_bytes(s.as_bytes().to_vec()).is_some());
        }

        for v in &model {
            prop_assert!(s.contains(*v));
            prop_assert!(s.search(*v).is_ok());
        }
        for (i, v) in model.iter().enumerate() {
            prop_assert_eq!(s.get(i), Some(*v));
        }
        prop_assert!(s.get(model.len()).is_none());
        // Reverse iteration.
        prop_assert_eq!(
            s.iter().rev().collect::<Vec<_>>(),
            model.iter().rev().copied().collect::<Vec<_>>()
        );
        if let Some(r) = s.random() {
            prop_assert!(model.contains(&r));
        }
    }
}

// ---------------------------------------------------------------------------
// quicklist
// ---------------------------------------------------------------------------

fn ql_value() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        8 => proptest::collection::vec(any::<u8>(), 0..40),
        2 => (0usize..300).prop_map(|n| vec![b'y'; n]),
        // Occasionally something that has to become a plain node at fill = -1.
        1 => Just(vec![b'B'; 9000]),
        2 => "[0-9-]{1,8}".prop_map(String::into_bytes),
    ]
}

#[derive(Debug, Clone)]
enum QlOp {
    PushHead(Vec<u8>),
    PushTail(Vec<u8>),
    PopHead,
    PopTail,
    Set(i64, Vec<u8>),
    InsertBefore(i64, Vec<u8>),
    InsertAfter(i64, Vec<u8>),
    DeleteRange(i64, usize),
}

fn ql_op() -> impl Strategy<Value = QlOp> {
    prop_oneof![
        5 => ql_value().prop_map(QlOp::PushHead),
        5 => ql_value().prop_map(QlOp::PushTail),
        3 => Just(QlOp::PopHead),
        3 => Just(QlOp::PopTail),
        2 => (-20i64..20, ql_value()).prop_map(|(i, v)| QlOp::Set(i, v)),
        2 => (-20i64..20, ql_value()).prop_map(|(i, v)| QlOp::InsertBefore(i, v)),
        2 => (-20i64..20, ql_value()).prop_map(|(i, v)| QlOp::InsertAfter(i, v)),
        2 => (-20i64..20, 0usize..6).prop_map(|(i, n)| QlOp::DeleteRange(i, n)),
    ]
}

/// Normalise a possibly-negative index the way the quicklist does.
fn norm(i: i64, len: usize) -> Option<usize> {
    let x = if i < 0 { i + len as i64 } else { i };
    if x < 0 || x as usize >= len {
        None
    } else {
        Some(x as usize)
    }
}

fn quicklist_model_check(fill: i64, ops: Vec<QlOp>) -> Result<(), TestCaseError> {
    let mut ql = Quicklist::new(fill);
    let mut model: Vec<Vec<u8>> = Vec::new();

    for op in ops {
        match op {
            QlOp::PushHead(v) => {
                ql.push_head(&v);
                model.insert(0, v);
            }
            QlOp::PushTail(v) => {
                ql.push_tail(&v);
                model.push(v);
            }
            QlOp::PopHead => {
                let got = ql.pop_head().map(|b| b.to_vec());
                let want = if model.is_empty() {
                    None
                } else {
                    Some(model.remove(0))
                };
                prop_assert_eq!(got, want);
            }
            QlOp::PopTail => {
                let got = ql.pop_tail().map(|b| b.to_vec());
                let want = model.pop();
                prop_assert_eq!(got, want);
            }
            QlOp::Set(i, v) => {
                let ok = ql.set(i, &v);
                match norm(i, model.len()) {
                    Some(k) => {
                        prop_assert!(ok);
                        model[k] = v;
                    }
                    None => prop_assert!(!ok),
                }
            }
            QlOp::InsertBefore(i, v) => {
                let ok = ql.insert_before(i, &v);
                match norm(i, model.len()) {
                    Some(k) => {
                        prop_assert!(ok);
                        model.insert(k, v);
                    }
                    None => prop_assert!(!ok),
                }
            }
            QlOp::InsertAfter(i, v) => {
                let ok = ql.insert_after(i, &v);
                match norm(i, model.len()) {
                    Some(k) => {
                        prop_assert!(ok);
                        model.insert(k + 1, v);
                    }
                    None => prop_assert!(!ok),
                }
            }
            QlOp::DeleteRange(i, n) => {
                let got = ql.delete_range(i, n);
                let want = match norm(i, model.len()) {
                    Some(k) if n > 0 => {
                        let c = n.min(model.len() - k);
                        model.drain(k..k + c);
                        c
                    }
                    _ => 0,
                };
                prop_assert_eq!(got, want);
            }
        }

        prop_assert_eq!(ql.len(), model.len());
        prop_assert_eq!(ql.is_empty(), model.is_empty());
        prop_assert_eq!(ql_dump(&ql), model.clone());
        // Reverse iteration must mirror forward iteration after every edit.
        prop_assert_eq!(ql_dump_rev(&ql), model.clone());
        prop_assert_eq!(ql.head().map(text), model.first().cloned());
        prop_assert_eq!(ql.tail().map(text), model.last().cloned());
        // No empty nodes may survive: Redis reaps them and so must we.
        prop_assert!(model.is_empty() == (ql.node_count() == 0));
    }

    for (i, want) in model.iter().enumerate() {
        prop_assert_eq!(ql.index(i as i64).map(text), Some(want.clone()));
        prop_assert_eq!(
            ql.index(i as i64 - model.len() as i64).map(text),
            Some(want.clone())
        );
        prop_assert_eq!(
            ql.iter_from(i as i64).map(text).collect::<Vec<_>>(),
            model[i..].to_vec()
        );
    }
    prop_assert!(ql.index(model.len() as i64).is_none());
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 120, ..ProptestConfig::default() })]

    /// Small count-based fill: forces node splits and merges constantly.
    #[test]
    fn prop_quicklist_matches_vec_model_count_fill(ops in proptest::collection::vec(ql_op(), 1..50)) {
        quicklist_model_check(3, ops)?;
    }

    /// Size-based fill, the shipping default shape (-2 is 8 KB; -1 is 4 KB and
    /// reaches the plain-node path with the 9000-byte values above).
    #[test]
    fn prop_quicklist_matches_vec_model_size_fill(ops in proptest::collection::vec(ql_op(), 1..50)) {
        quicklist_model_check(-1, ops)?;
    }

    #[test]
    fn prop_quicklist_lrem_matches_model(
        vals in proptest::collection::vec("[abc]", 0..30),
        needle in "[abc]",
        count in -4i64..5,
    ) {
        let mut ql = Quicklist::from_values(3, vals.iter().map(String::as_bytes));
        let mut model: Vec<Vec<u8>> = vals.iter().map(|s| s.as_bytes().to_vec()).collect();
        let n = needle.as_bytes();

        let removed = ql.remove_value(n, count);

        let want = if count == 0 {
            let before = model.len();
            model.retain(|v| v != n);
            before - model.len()
        } else if count > 0 {
            let mut left = count as usize;
            let mut got = 0;
            model.retain(|v| {
                if left > 0 && v == n { left -= 1; got += 1; false } else { true }
            });
            got
        } else {
            let mut left = count.unsigned_abs() as usize;
            let mut got = 0;
            let mut kept: Vec<Vec<u8>> = Vec::new();
            for v in model.iter().rev() {
                if left > 0 && v == n { left -= 1; got += 1; } else { kept.push(v.clone()); }
            }
            kept.reverse();
            model = kept;
            got
        };
        prop_assert_eq!(removed, want);
        prop_assert_eq!(ql_dump(&ql), model);
    }
}

// ---------------------------------------------------------------------------
// skiplist
// ---------------------------------------------------------------------------

fn score() -> impl Strategy<Value = f64> {
    prop_oneof![
        Just(0.0f64),
        Just(-0.0f64),
        Just(1.0),
        Just(-1.0),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        (-5i32..5).prop_map(f64::from),
        (-1000i32..1000).prop_map(|v| f64::from(v) / 4.0),
    ]
}

fn member() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        "[a-e]{0,3}".prop_map(String::into_bytes),
        proptest::collection::vec(any::<u8>(), 0..4),
    ]
}

/// The model's ordering: score as an `f64` total order (no NaNs are ever
/// inserted), ties broken by memcmp on the member. Exactly `zslInsert`.
#[derive(Debug, Clone, PartialEq)]
struct ModelKey(f64, Vec<u8>);

impl Eq for ModelKey {}
impl Ord for ModelKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0
            .partial_cmp(&other.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| self.1.cmp(&other.1))
    }
}
impl PartialOrd for ModelKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 160, ..ProptestConfig::default() })]

    #[test]
    fn prop_skiplist_matches_btreeset_model(
        ops in proptest::collection::vec((0u8..3, score(), member()), 1..90)
    ) {
        let mut sl = Skiplist::new();
        // member -> score, mirroring the dict half of a real zset.
        let mut dict: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
        let mut model: BTreeSet<ModelKey> = BTreeSet::new();

        for (kind, s, m) in ops {
            match kind {
                0 => {
                    // Insert or update, exactly as ZADD does.
                    if let Some(old) = dict.insert(m.clone(), s) {
                        model.remove(&ModelKey(old, m.clone()));
                        prop_assert!(sl.update_score(old, &m, s));
                    } else {
                        sl.insert(s, Bytes::from(m.clone()));
                    }
                    model.insert(ModelKey(s, m));
                }
                1 => {
                    if let Some(old) = dict.remove(&m) {
                        prop_assert!(sl.delete(old, &m));
                        model.remove(&ModelKey(old, m));
                    } else {
                        // Deleting an absent member must be a clean false.
                        prop_assert!(!sl.delete(s, &m));
                    }
                }
                _ => {
                    if let Some(&old) = dict.get(&m) {
                        prop_assert!(sl.update_score(old, &m, s));
                        model.remove(&ModelKey(old, m.clone()));
                        model.insert(ModelKey(s, m.clone()));
                        dict.insert(m, s);
                    }
                }
            }

            let want: Vec<(f64, Vec<u8>)> =
                model.iter().map(|k| (k.0, k.1.clone())).collect();
            prop_assert_eq!(sl.len(), want.len());
            prop_assert_eq!(sl_dump(&sl), want.clone());
            // Reverse iteration must mirror forward iteration.
            let mut rev = sl.iter_rev().map(|(m, s)| (s, m.to_vec())).collect::<Vec<_>>();
            rev.reverse();
            prop_assert_eq!(rev, want.clone());
            prop_assert_eq!(sl.first().map(|(m, s)| (s, m.to_vec())), want.first().cloned());
            prop_assert_eq!(sl.last().map(|(m, s)| (s, m.to_vec())), want.last().cloned());

            // Ranks are dense, 0-based and round-trip through by_rank.
            for (i, (s, m)) in want.iter().enumerate() {
                prop_assert_eq!(sl.rank_of(*s, m), Some(i));
                prop_assert_eq!(sl.by_rank(i).map(|(m, s)| (s, m.to_vec())), Some((*s, m.clone())));
            }
            prop_assert!(sl.by_rank(want.len()).is_none());
        }
    }

    #[test]
    fn prop_skiplist_score_ranges_match_a_filter(
        items in proptest::collection::vec((score(), "[a-h]{1,3}"), 0..40),
        lo in score(), hi in score(), lo_ex in any::<bool>(), hi_ex in any::<bool>(),
    ) {
        let mut sl = Skiplist::new();
        let mut dict: BTreeMap<Vec<u8>, f64> = BTreeMap::new();
        for (s, m) in &items {
            let mb = m.as_bytes().to_vec();
            if let Some(old) = dict.insert(mb.clone(), *s) {
                sl.update_score(old, &mb, *s);
            } else {
                sl.insert(*s, Bytes::from(mb));
            }
        }
        let mut sorted: Vec<(f64, Vec<u8>)> =
            dict.iter().map(|(m, s)| (*s, m.clone())).collect();
        sorted.sort_by(|a, b| ModelKey(a.0, a.1.clone()).cmp(&ModelKey(b.0, b.1.clone())));

        let r = ScoreRange::new(
            if lo_ex { ScoreBound::excl(lo) } else { ScoreBound::incl(lo) },
            if hi_ex { ScoreBound::excl(hi) } else { ScoreBound::incl(hi) },
        );
        let want: Vec<(f64, Vec<u8>)> = sorted
            .iter()
            .filter(|(s, _)| r.contains(*s))
            .cloned()
            .collect();
        let got: Vec<(f64, Vec<u8>)> = sl.range(&r).map(|(m, s)| (s, m.to_vec())).collect();
        prop_assert_eq!(got, want.clone());

        prop_assert_eq!(
            sl.first_in_range(&r),
            want.first().and_then(|(s, m)| sl.rank_of(*s, m))
        );
        prop_assert_eq!(
            sl.last_in_range(&r),
            want.last().and_then(|(s, m)| sl.rank_of(*s, m))
        );

        // Bulk delete removes exactly the same set.
        let mut removed = Vec::new();
        let n = sl.delete_range_by_score(&r, |m, s| removed.push((s, m.to_vec())));
        prop_assert_eq!(n, want.len());
        prop_assert_eq!(removed, want.clone());
        prop_assert_eq!(sl.len(), sorted.len() - want.len());
    }

    #[test]
    fn prop_skiplist_lex_ranges_match_a_filter(
        members in proptest::collection::vec("[a-f]{0,3}", 0..30),
        lo in "[a-f]{0,3}", hi in "[a-f]{0,3}",
        lo_kind in 0u8..3, hi_kind in 0u8..3,
    ) {
        // ZRANGEBYLEX is only defined when every member shares one score.
        let mut sl = Skiplist::new();
        let mut set: BTreeSet<Vec<u8>> = BTreeSet::new();
        for m in &members {
            let mb = m.as_bytes().to_vec();
            if set.insert(mb.clone()) {
                sl.insert(0.0, Bytes::from(mb));
            }
        }

        let bound = |kind: u8, s: &str, neg: bool| match kind {
            0 => if neg { LexBound::NegInf } else { LexBound::PosInf },
            1 => LexBound::Incl(Bytes::copy_from_slice(s.as_bytes())),
            _ => LexBound::Excl(Bytes::copy_from_slice(s.as_bytes())),
        };
        let r = LexRange::new(bound(lo_kind, &lo, true), bound(hi_kind, &hi, false));

        let want: Vec<Vec<u8>> = set.iter().filter(|m| r.contains(m)).cloned().collect();
        let got: Vec<Vec<u8>> = sl.lex_range(&r).map(|(m, _)| m.to_vec()).collect();
        prop_assert_eq!(got, want.clone());

        let mut removed = Vec::new();
        let n = sl.delete_range_by_lex(&r, |m, _| removed.push(m.to_vec()));
        prop_assert_eq!(n, want.len());
        prop_assert_eq!(removed, want.clone());
        prop_assert_eq!(sl.len(), set.len() - want.len());
    }

    #[test]
    fn prop_skiplist_delete_by_rank_matches_a_drain(
        n in 0usize..40, start in 0usize..45, count in 0usize..12,
    ) {
        let mut sl = Skiplist::new();
        for i in 0..n {
            sl.insert(i as f64, Bytes::from(format!("m{i:03}")));
        }
        let mut model: Vec<usize> = (0..n).collect();

        let got = sl.delete_range_by_rank(start, count, |_, _| {});
        let want = if start >= n || count == 0 { 0 } else { count.min(n - start) };
        prop_assert_eq!(got, want);
        if want > 0 {
            model.drain(start..start + want);
        }
        prop_assert_eq!(sl.len(), model.len());
        prop_assert_eq!(
            sl.iter().map(|(_, s)| s as usize).collect::<Vec<_>>(),
            model
        );
    }
}

// ---------------------------------------------------------------------------
// rax
// ---------------------------------------------------------------------------

fn rax_key() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        // Short alphabet keys: maximum prefix sharing, so splits and merges
        // fire on nearly every operation.
        "[ab]{0,6}".prop_map(String::into_bytes),
        "[a-d]{0,4}".prop_map(String::into_bytes),
        // Long shared prefixes, the stream-ID shape.
        "prefix:[0-9]{1,4}".prop_map(String::into_bytes),
        proptest::collection::vec(any::<u8>(), 0..6),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

    #[test]
    fn prop_rax_matches_btreemap_model(
        ops in proptest::collection::vec((any::<bool>(), rax_key(), any::<u32>()), 1..120)
    ) {
        let mut r: Rax<u32> = Rax::new();
        let mut model: BTreeMap<Vec<u8>, u32> = BTreeMap::new();

        for (insert, k, v) in ops {
            if insert {
                prop_assert_eq!(r.insert(&k, v), model.insert(k.clone(), v));
            } else {
                prop_assert_eq!(r.remove(&k), model.remove(&k));
            }

            prop_assert_eq!(r.len(), model.len());
            prop_assert_eq!(r.is_empty(), model.is_empty());
            prop_assert_eq!(r.find(&k), model.get(&k));
            // Full ordered traversal after every single operation: this is
            // what catches a botched split or a merge that ate a key.
            let want: Vec<(Vec<u8>, u32)> =
                model.iter().map(|(k, v)| (k.clone(), *v)).collect();
            prop_assert_eq!(rax_dump(&r), want.clone());

            let mut rev = Vec::new();
            let mut it = r.iter_rev();
            while let Some((k, v)) = it.next() {
                rev.push((k.to_vec(), *v));
            }
            rev.reverse();
            prop_assert_eq!(rev, want);
        }

        for (k, v) in &model {
            prop_assert_eq!(r.find(k), Some(v));
            prop_assert!(r.contains_key(k));
        }
        prop_assert_eq!(
            r.first().map(|(k, v)| (k.to_vec(), *v)),
            model.iter().next().map(|(k, v)| (k.clone(), *v))
        );
        prop_assert_eq!(
            r.last().map(|(k, v)| (k.to_vec(), *v)),
            model.iter().next_back().map(|(k, v)| (k.clone(), *v))
        );
    }

    #[test]
    fn prop_rax_seek_matches_btreemap_bounds(
        keys in proptest::collection::vec(rax_key(), 0..40),
        target in rax_key(),
    ) {
        let mut r: Rax<u32> = Rax::new();
        let mut model: BTreeMap<Vec<u8>, u32> = BTreeMap::new();
        for (i, k) in keys.iter().enumerate() {
            r.insert(k, i as u32);
            model.insert(k.clone(), i as u32);
        }

        let first = |op| {
            let mut it = r.seek(op, &target);
            it.next().map(|(k, _)| k.to_vec())
        };
        let last = |op| {
            let mut it = r.seek_rev(op, &target);
            it.next().map(|(k, _)| k.to_vec())
        };

        prop_assert_eq!(first(Seek::Ge), model.range(target.clone()..).next().map(|(k, _)| k.clone()));
        prop_assert_eq!(
            first(Seek::Gt),
            model
                .range((std::ops::Bound::Excluded(target.clone()), std::ops::Bound::Unbounded))
                .next()
                .map(|(k, _)| k.clone())
        );
        prop_assert_eq!(
            last(Seek::Le),
            model.range(..=target.clone()).next_back().map(|(k, _)| k.clone())
        );
        prop_assert_eq!(
            last(Seek::Lt),
            model.range(..target.clone()).next_back().map(|(k, _)| k.clone())
        );

        // And iterating on from a seek must equal the model's range scan.
        let mut fwd = Vec::new();
        let mut it = r.seek(Seek::Ge, &target);
        while let Some((k, _)) = it.next() {
            fwd.push(k.to_vec());
        }
        prop_assert_eq!(fwd, model.range(target.clone()..).map(|(k, _)| k.clone()).collect::<Vec<_>>());

        let mut bwd = Vec::new();
        let mut it = r.seek_rev(Seek::Le, &target);
        while let Some((k, _)) = it.next() {
            bwd.push(k.to_vec());
        }
        let mut want: Vec<Vec<u8>> = model.range(..=target).map(|(k, _)| k.clone()).collect();
        want.reverse();
        prop_assert_eq!(bwd, want);
    }
}

// ---------------------------------------------------------------------------
// fixed nasty cases the generators would only rarely produce
// ---------------------------------------------------------------------------

#[test]
fn listpack_int_encoding_boundaries_exactly() {
    // (value, expected bytes of encoding + data, per lpEncodeGetType)
    let cases: &[(i64, usize)] = &[
        (0, 1),
        (127, 1),
        (128, 2),
        (-1, 2),
        (4095, 2),
        (-4096, 2),
        (4096, 3),
        (-4097, 3),
        (32767, 3),
        (-32768, 3),
        (32768, 4),
        (-32769, 4),
        (8_388_607, 4),
        (-8_388_608, 4),
        (8_388_608, 5),
        (-8_388_609, 5),
        (2_147_483_647, 5),
        (-2_147_483_648, 5),
        (2_147_483_648, 9),
        (-2_147_483_649, 9),
        (i64::MAX, 9),
        (i64::MIN, 9),
    ];
    for &(v, enclen) in cases {
        let mut lp = Listpack::new();
        assert!(lp.append(ListpackEntry::Int(v)));
        assert_eq!(lp.get(0).and_then(|e| e.as_int()), Some(v), "value {v}");
        // 7 = empty listpack; backlen is 1 byte for every enclen here.
        assert_eq!(lp.total_bytes(), 7 + enclen + 1, "width of {v}");
        assert!(Listpack::validate(lp.as_bytes()));
        // Reached through the string path too, since Redis coerces.
        let mut lp2 = Listpack::new();
        assert!(lp2.append(ListpackEntry::Str(v.to_string().as_bytes())));
        assert_eq!(lp2.as_bytes(), lp.as_bytes(), "string coercion of {v}");
    }
}

#[test]
fn listpack_string_length_boundaries_exactly() {
    for (n, header) in [(0usize, 1usize), (63, 1), (64, 2), (4095, 2), (4096, 5)] {
        let data = vec![b'x'; n];
        let mut lp = Listpack::new();
        assert!(lp.append(ListpackEntry::Str(&data)));
        let enclen = header + n;
        let backlen = if enclen <= 127 {
            1
        } else if enclen < 16383 {
            2
        } else {
            3
        };
        assert_eq!(lp.total_bytes(), 7 + enclen + backlen, "n = {n}");
        assert_eq!(lp.get(0).and_then(|e| e.as_bytes()), Some(&data[..]));
        assert!(Listpack::validate(lp.as_bytes()));
    }
}

#[test]
fn listpack_backlen_two_to_three_byte_boundary() {
    // A 16382-byte entry needs a 2-byte backlen; 16383 needs 3, because of
    // the `<` in lpEncodeBacklen. Both must survive a reverse walk.
    for n in [16_377usize, 16_378, 16_379] {
        let data = vec![b'z'; n];
        let mut lp = Listpack::new();
        lp.append(ListpackEntry::Str(b"head"));
        lp.append(ListpackEntry::Str(&data));
        lp.append(ListpackEntry::Str(b"tail"));
        assert!(Listpack::validate(lp.as_bytes()), "n = {n}");
        assert_eq!(
            lp_dump(&lp),
            lp_dump_rev(&lp),
            "reverse walk broke at n = {n}"
        );
        assert_eq!(lp.get(-1).map(text), Some(b"tail".to_vec()));
        assert_eq!(
            lp.get(1).and_then(|e| e.as_bytes()).map(<[u8]>::len),
            Some(n)
        );
    }
}

#[test]
fn every_structure_survives_empty_and_single() {
    // Listpack.
    let mut lp = Listpack::new();
    assert!(lp.is_empty() && lp.first().is_none());
    assert_eq!(lp.len(), 0);
    assert!(!lp.delete(0) && lp.delete_range(0, 1) == 0 && !lp.replace(0, ListpackEntry::Int(1)));
    lp.append(ListpackEntry::Int(1));
    assert_eq!(lp.len(), 1);
    assert!(lp.delete(0) && lp.is_empty());

    // Intset.
    let mut is = Intset::new();
    assert!(is.is_empty() && !is.remove(1) && !is.contains(1));
    assert!(is.add(1) && is.len() == 1);
    assert!(is.remove(1) && is.is_empty());

    // Quicklist.
    let mut ql = Quicklist::new(128);
    assert!(ql.is_empty() && ql.pop_head().is_none() && ql.pop_tail().is_none());
    ql.push_tail(b"x");
    assert_eq!(ql.len(), 1);
    assert_eq!(ql.pop_tail().as_deref(), Some(&b"x"[..]));
    assert!(ql.is_empty() && ql.node_count() == 0);

    // Skiplist.
    let mut sl = Skiplist::new();
    assert!(sl.is_empty() && !sl.delete(0.0, b"x"));
    sl.insert(1.0, Bytes::from_static(b"x"));
    assert_eq!(sl.rank_of(1.0, b"x"), Some(0));
    assert!(sl.delete(1.0, b"x") && sl.is_empty());

    // Rax.
    let mut r: Rax<u8> = Rax::new();
    assert!(r.is_empty() && r.remove(b"x").is_none());
    r.insert(b"x", 1);
    assert_eq!(r.find(b"x"), Some(&1));
    assert_eq!(r.remove(b"x"), Some(1));
    assert!(r.is_empty() && r.node_count() == 1);
}

#[test]
fn quicklist_reverse_iteration_after_deleting_the_last_element() {
    for fill in [1i64, 2, 3, 128, -1, -2] {
        let mut ql = Quicklist::from_values(fill, (0..17).map(|i| format!("v{i:02}")));
        while ql.len() > 1 {
            ql.drop_tail();
            let fwd = ql_dump(&ql);
            assert_eq!(ql_dump_rev(&ql), fwd, "fill = {fill}, len = {}", ql.len());
        }
        ql.drop_tail();
        assert!(ql.is_empty(), "fill = {fill}");
        assert_eq!(ql.node_count(), 0, "fill = {fill}");
        assert_eq!(ql.iter_rev().count(), 0);
    }
}

#[test]
fn rax_deleting_the_last_key_leaves_a_bare_root() {
    let mut r: Rax<u32> = Rax::new();
    for k in ["aaa", "aab", "abc", "b", ""] {
        r.insert(k.as_bytes(), 1);
    }
    for k in ["aaa", "aab", "abc", "b", ""] {
        assert_eq!(r.remove(k.as_bytes()), Some(1), "removing {k:?}");
    }
    assert!(r.is_empty());
    assert_eq!(r.node_count(), 1, "the tree must prune back to the root");
    assert!(r.iter().next().is_none());
    assert!(r.iter_rev().next().is_none());
}

#[test]
fn skiplist_lex_ordering_is_memcmp_then_length() {
    // The exact ordering ZRANGEBYLEX depends on. "" < "a" < "ab" < "b".
    let mut sl = Skiplist::new();
    for m in ["b", "ab", "", "a", "aa"] {
        sl.insert(0.0, Bytes::copy_from_slice(m.as_bytes()));
    }
    let got: Vec<String> = sl
        .iter()
        .map(|(m, _)| String::from_utf8_lossy(m).into_owned())
        .collect();
    assert_eq!(got, vec!["", "a", "aa", "ab", "b"]);

    // High bytes sort above ASCII: memcmp is unsigned.
    let mut sl = Skiplist::new();
    for m in [vec![0x7fu8], vec![0x80], vec![0xff], vec![0x00]] {
        sl.insert(0.0, Bytes::from(m));
    }
    let got: Vec<u8> = sl.iter().map(|(m, _)| m[0]).collect();
    assert_eq!(got, vec![0x00, 0x7f, 0x80, 0xff]);
}
