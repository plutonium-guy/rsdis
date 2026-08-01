//! Skiplist -- the sorted-set index above `zset-max-listpack-entries`.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! Paired with a member -> score dict, exactly as in `t_zset.c`, so that
//! `ZSCORE` is O(1) while `ZRANGEBYSCORE` and `ZRANK` stay O(log n). Level
//! generation uses p = 0.25 and a 32-level cap, matching Redis, so that
//! `DEBUG JMAP`-style structural comparisons line up.
//!
//! # Ordering
//!
//! Elements are ordered by `(score, member)`: score compared as `f64`, ties
//! broken by memcmp on the member bytes. That tie-break is not a detail --
//! `ZRANGEBYLEX` is only meaningful when every member shares one score, and it
//! reads the list in exactly this order. Rust's `Ord for [u8]` is memcmp then
//! length, which is what `sdscmp` does.
//!
//! # Representation
//!
//! An arena (`Vec<Node>`) with `u32` indices rather than raw pointers. Node 0
//! is the header sentinel (32 levels, no member). Deleted slots go on a free
//! list and are reused. This keeps the module free of `unsafe` at no cost:
//! index arithmetic compiles to the same address arithmetic a pointer chase
//! would, and the arena is contiguous, which pointer-per-node allocation is
//! not.
//!
//! Spans and forward links are `u32`, capping a single sorted set at
//! `u32::MAX - 1` members. Redis uses `unsigned long`; 4 billion members in
//! one key is not a workload this server is trying to serve, and halving the
//! per-level cost from 16 to 8 bytes matters much more.
//!
//! Members are `bytes::Bytes` so the paired dict in `types/zset.rs` (W2c) can
//! hold the same buffer without a second copy, exactly as `t_zset.c` shares
//! one `sds` between the dict and the skiplist.

use bytes::Bytes;
use smallvec::SmallVec;

/// `ZSKIPLIST_MAXLEVEL`.
pub const MAX_LEVEL: usize = 32;
/// `ZSKIPLIST_P`.
pub const P: f64 = 0.25;

/// Sentinel for "no node".
const NIL: u32 = u32::MAX;
/// The header sentinel's arena index.
const HEAD: u32 = 0;

/// Threshold for the level coin flip, precomputed against `u32::MAX`.
const LEVEL_THRESHOLD: u32 = (P * (u32::MAX as f64)) as u32;

#[derive(Debug, Clone, Copy)]
struct Level {
    forward: u32,
    span: u32,
}

const NIL_LEVEL: Level = Level {
    forward: NIL,
    span: 0,
};

#[derive(Debug, Clone)]
struct Node {
    member: Bytes,
    score: f64,
    backward: u32,
    /// One entry per level. `SmallVec<[Level; 2]>` is the same 24 bytes as
    /// `SmallVec<[Level; 1]>` would be, and covers ~94% of nodes inline.
    levels: SmallVec<[Level; 2]>,
}

/// Inclusive/exclusive score bound.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreBound {
    pub value: f64,
    /// True when the bound is exclusive (`ZRANGEBYSCORE (5`).
    pub exclusive: bool,
}

impl ScoreBound {
    #[inline]
    pub fn incl(v: f64) -> Self {
        ScoreBound {
            value: v,
            exclusive: false,
        }
    }
    #[inline]
    pub fn excl(v: f64) -> Self {
        ScoreBound {
            value: v,
            exclusive: true,
        }
    }
}

/// `t_zset.c: zrangespec`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoreRange {
    pub min: ScoreBound,
    pub max: ScoreBound,
}

impl ScoreRange {
    #[inline]
    pub fn new(min: ScoreBound, max: ScoreBound) -> Self {
        ScoreRange { min, max }
    }

    /// `-inf..=+inf`, the whole set.
    #[inline]
    pub fn all() -> Self {
        ScoreRange {
            min: ScoreBound::incl(f64::NEG_INFINITY),
            max: ScoreBound::incl(f64::INFINITY),
        }
    }

    /// `t_zset.c: zslIsInRange`'s empty check.
    #[inline]
    pub fn is_empty(&self) -> bool {
        if self.min.value > self.max.value {
            return true;
        }
        self.min.value == self.max.value && (self.min.exclusive || self.max.exclusive)
    }

    #[inline]
    fn gte_min(&self, v: f64) -> bool {
        if self.min.exclusive {
            v > self.min.value
        } else {
            v >= self.min.value
        }
    }

    #[inline]
    fn lte_max(&self, v: f64) -> bool {
        if self.max.exclusive {
            v < self.max.value
        } else {
            v <= self.max.value
        }
    }

    #[inline]
    pub fn contains(&self, v: f64) -> bool {
        self.gte_min(v) && self.lte_max(v)
    }
}

/// One end of a `ZRANGEBYLEX` range.
///
/// `-` and `+` in the command syntax become [`LexBound::NegInf`] and
/// [`LexBound::PosInf`]; `[foo` and `(foo` become the inclusive and exclusive
/// variants. Parsing lives with the command (W2c); this type is what the
/// skiplist consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LexBound {
    /// `-`: smaller than every possible member.
    NegInf,
    /// `+`: larger than every possible member.
    PosInf,
    Incl(Bytes),
    Excl(Bytes),
}

/// `t_zset.c: zlexrangespec`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexRange {
    pub min: LexBound,
    pub max: LexBound,
}

impl LexRange {
    #[inline]
    pub fn new(min: LexBound, max: LexBound) -> Self {
        LexRange { min, max }
    }

    /// The whole set (`- +`).
    #[inline]
    pub fn all() -> Self {
        LexRange {
            min: LexBound::NegInf,
            max: LexBound::PosInf,
        }
    }

    /// `t_zset.c: zslIsInLexRange`'s empty check.
    pub fn is_empty(&self) -> bool {
        match (&self.min, &self.max) {
            (LexBound::PosInf, _) | (_, LexBound::NegInf) => true,
            (LexBound::NegInf, _) | (_, LexBound::PosInf) => false,
            (LexBound::Incl(a) | LexBound::Excl(a), LexBound::Incl(b) | LexBound::Excl(b)) => {
                let cmp = a.as_ref().cmp(b.as_ref());
                cmp == std::cmp::Ordering::Greater
                    || (cmp == std::cmp::Ordering::Equal
                        && (matches!(self.min, LexBound::Excl(_))
                            || matches!(self.max, LexBound::Excl(_))))
            }
        }
    }

    #[inline]
    fn gte_min(&self, m: &[u8]) -> bool {
        match &self.min {
            LexBound::NegInf => true,
            LexBound::PosInf => false,
            LexBound::Incl(b) => m >= b.as_ref(),
            LexBound::Excl(b) => m > b.as_ref(),
        }
    }

    #[inline]
    fn lte_max(&self, m: &[u8]) -> bool {
        match &self.max {
            LexBound::PosInf => true,
            LexBound::NegInf => false,
            LexBound::Incl(b) => m <= b.as_ref(),
            LexBound::Excl(b) => m < b.as_ref(),
        }
    }

    #[inline]
    pub fn contains(&self, m: &[u8]) -> bool {
        self.gte_min(m) && self.lte_max(m)
    }
}

/// `t_zset.c: zslRandomLevel`.
#[inline]
fn random_level() -> usize {
    let mut level = 1usize;
    while crate::util::rand::u32_() < LEVEL_THRESHOLD {
        level += 1;
        if level >= MAX_LEVEL {
            return MAX_LEVEL;
        }
    }
    level
}

/// The skiplist half of a sorted set.
#[derive(Debug, Clone)]
pub struct Skiplist {
    nodes: Vec<Node>,
    free: Vec<u32>,
    tail: u32,
    level: usize,
    length: usize,
}

impl Default for Skiplist {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Skiplist {
    /// `zslCreate`.
    pub fn new() -> Self {
        let header = Node {
            member: Bytes::new(),
            score: 0.0,
            backward: NIL,
            levels: SmallVec::from_elem(NIL_LEVEL, MAX_LEVEL),
        };
        Skiplist {
            nodes: vec![header],
            free: Vec::new(),
            tail: NIL,
            level: 1,
            length: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.length
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Current top level in use, for tests and `DEBUG`-style introspection.
    #[inline]
    pub fn level(&self) -> usize {
        self.level
    }

    /// Approximate heap footprint, for `MEMORY USAGE`.
    pub fn mem_usage(&self) -> usize {
        let per_node = core::mem::size_of::<Node>();
        core::mem::size_of::<Self>()
            + self.nodes.capacity() * per_node
            + self.free.capacity() * 4
            // Levels beyond the two inline slots, plus the header's 32.
            + self
                .nodes
                .iter()
                .map(|n| {
                    if n.levels.spilled() {
                        n.levels.capacity() * core::mem::size_of::<Level>()
                    } else {
                        0
                    }
                })
                .sum::<usize>()
    }

    #[inline]
    fn node(&self, i: u32) -> &Node {
        &self.nodes[i as usize]
    }

    #[inline]
    fn lvl(&self, i: u32, l: usize) -> Level {
        self.nodes[i as usize].levels[l]
    }

    #[inline]
    fn set_lvl(&mut self, i: u32, l: usize, v: Level) {
        self.nodes[i as usize].levels[l] = v;
    }

    /// True when `(score, member)` sorts strictly before the node at `i`.
    /// `i` must not be the header.
    #[inline]
    fn node_lt(&self, i: u32, score: f64, member: &[u8]) -> bool {
        let n = self.node(i);
        n.score < score || (n.score == score && n.member.as_ref() < member)
    }

    fn alloc(&mut self, member: Bytes, score: f64, level: usize) -> u32 {
        let node = Node {
            member,
            score,
            backward: NIL,
            levels: SmallVec::from_elem(NIL_LEVEL, level),
        };
        match self.free.pop() {
            Some(i) => {
                self.nodes[i as usize] = node;
                i
            }
            None => {
                let i = self.nodes.len() as u32;
                self.nodes.push(node);
                i
            }
        }
    }

    fn release(&mut self, i: u32) {
        let n = &mut self.nodes[i as usize];
        n.member = Bytes::new();
        n.score = 0.0;
        n.backward = NIL;
        n.levels.clear();
        self.free.push(i);
    }

    // ---- insert / delete -----------------------------------------------

    /// `zslInsert`. The caller (W2c's zset) guarantees `member` is not already
    /// present -- that is what the paired dict is for. Score must not be NaN;
    /// Redis rejects NaN scores at the command layer.
    pub fn insert(&mut self, score: f64, member: Bytes) {
        let mut update = [HEAD; MAX_LEVEL];
        let mut rank = [0usize; MAX_LEVEL];

        let mut x = HEAD;
        for i in (0..self.level).rev() {
            rank[i] = if i + 1 == self.level { 0 } else { rank[i + 1] };
            loop {
                let f = self.lvl(x, i).forward;
                if f != NIL && self.node_lt(f, score, member.as_ref()) {
                    rank[i] += self.lvl(x, i).span as usize;
                    x = f;
                } else {
                    break;
                }
            }
            update[i] = x;
        }

        let new_level = random_level();
        if new_level > self.level {
            for (i, u) in update
                .iter_mut()
                .enumerate()
                .take(new_level)
                .skip(self.level)
            {
                rank[i] = 0;
                *u = HEAD;
                self.set_lvl(
                    HEAD,
                    i,
                    Level {
                        forward: NIL,
                        span: self.length as u32,
                    },
                );
            }
            self.level = new_level;
        }

        let x = self.alloc(member, score, new_level);
        for i in 0..new_level {
            let up = update[i];
            let prev = self.lvl(up, i);
            let step = (rank[0] - rank[i]) as u32;
            self.set_lvl(
                x,
                i,
                Level {
                    forward: prev.forward,
                    span: prev.span - step,
                },
            );
            self.set_lvl(
                up,
                i,
                Level {
                    forward: x,
                    span: step + 1,
                },
            );
        }
        // Levels above the new node's height gain one element of span.
        for (i, &up) in update.iter().enumerate().take(self.level).skip(new_level) {
            let mut l = self.lvl(up, i);
            l.span += 1;
            self.set_lvl(up, i, l);
        }

        let back = if update[0] == HEAD { NIL } else { update[0] };
        self.nodes[x as usize].backward = back;
        let fwd = self.lvl(x, 0).forward;
        if fwd != NIL {
            self.nodes[fwd as usize].backward = x;
        } else {
            self.tail = x;
        }
        self.length += 1;
    }

    /// Locate `(score, member)` and fill `update` with the predecessor at
    /// every level. Returns the node index when found.
    fn find_with_update(
        &self,
        score: f64,
        member: &[u8],
        update: &mut [u32; MAX_LEVEL],
    ) -> Option<u32> {
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.lvl(x, i).forward;
                if f != NIL && self.node_lt(f, score, member) {
                    x = f;
                } else {
                    break;
                }
            }
            update[i] = x;
        }
        let cand = self.lvl(x, 0).forward;
        if cand != NIL {
            let n = self.node(cand);
            if n.score == score && n.member.as_ref() == member {
                return Some(cand);
            }
        }
        None
    }

    /// `zslDeleteNode`: unlink `x`, whose predecessors are in `update`.
    fn unlink(&mut self, x: u32, update: &[u32; MAX_LEVEL]) {
        for (i, &up) in update.iter().enumerate().take(self.level) {
            let mut ul = self.lvl(up, i);
            if ul.forward == x {
                let xl = self.lvl(x, i);
                ul.span += xl.span;
                ul.span -= 1;
                ul.forward = xl.forward;
            } else {
                ul.span -= 1;
            }
            self.set_lvl(up, i, ul);
        }
        let fwd = self.lvl(x, 0).forward;
        let back = self.node(x).backward;
        if fwd != NIL {
            self.nodes[fwd as usize].backward = back;
        } else {
            self.tail = back;
        }
        while self.level > 1 && self.lvl(HEAD, self.level - 1).forward == NIL {
            self.level -= 1;
        }
        self.length -= 1;
    }

    /// `zslDelete`. Returns true when the element was present.
    pub fn delete(&mut self, score: f64, member: &[u8]) -> bool {
        let mut update = [HEAD; MAX_LEVEL];
        let Some(x) = self.find_with_update(score, member, &mut update) else {
            return false;
        };
        self.unlink(x, &update);
        self.release(x);
        true
    }

    /// `zslUpdateScore`. Returns false when `(cur_score, member)` is absent.
    ///
    /// Redis keeps the node in place when the reordering is a no-op, which
    /// avoids the allocation of a delete+insert. We do the same: the node
    /// stays if it is still between its neighbours.
    pub fn update_score(&mut self, cur_score: f64, member: &[u8], new_score: f64) -> bool {
        let mut update = [HEAD; MAX_LEVEL];
        let Some(x) = self.find_with_update(cur_score, member, &mut update) else {
            return false;
        };

        let back = self.node(x).backward;
        let fwd = self.lvl(x, 0).forward;
        let ok_left = back == NIL || {
            let b = self.node(back);
            b.score < new_score || (b.score == new_score && b.member.as_ref() < member)
        };
        let ok_right = fwd == NIL || {
            let f = self.node(fwd);
            f.score > new_score || (f.score == new_score && f.member.as_ref() > member)
        };
        if ok_left && ok_right {
            self.nodes[x as usize].score = new_score;
            return true;
        }

        let m = self.node(x).member.clone();
        self.unlink(x, &update);
        self.release(x);
        self.insert(new_score, m);
        true
    }

    /// Remove everything, keeping the arena allocation.
    pub fn clear(&mut self) {
        self.nodes.truncate(1);
        self.free.clear();
        if let Some(h) = self.nodes.first_mut() {
            h.levels.clear();
            h.levels.extend(std::iter::repeat_n(NIL_LEVEL, MAX_LEVEL));
        }
        self.tail = NIL;
        self.level = 1;
        self.length = 0;
    }

    // ---- rank ----------------------------------------------------------

    /// `zslGetRank`, but 0-based: rank 0 is the lowest-scoring member.
    /// Returns `None` when the element is absent.
    pub fn rank_of(&self, score: f64, member: &[u8]) -> Option<usize> {
        let mut x = HEAD;
        let mut rank = 0usize;
        for i in (0..self.level).rev() {
            loop {
                let f = self.lvl(x, i).forward;
                if f != NIL && {
                    let n = self.node(f);
                    n.score < score || (n.score == score && n.member.as_ref() <= member)
                } {
                    rank += self.lvl(x, i).span as usize;
                    x = f;
                } else {
                    break;
                }
            }
            if x != HEAD && self.node(x).member.as_ref() == member && self.node(x).score == score {
                return Some(rank - 1);
            }
        }
        None
    }

    /// `zslGetElementByRank`, 0-based. `None` when out of range.
    pub fn by_rank(&self, rank: usize) -> Option<(&Bytes, f64)> {
        let idx = self.node_by_rank(rank)?;
        let n = self.node(idx);
        Some((&n.member, n.score))
    }

    fn node_by_rank(&self, rank: usize) -> Option<u32> {
        if rank >= self.length {
            return None;
        }
        // Internally Redis ranks from 1 because the header occupies rank 0.
        let target = rank + 1;
        let mut x = HEAD;
        let mut traversed = 0usize;
        for i in (0..self.level).rev() {
            loop {
                let l = self.lvl(x, i);
                if l.forward != NIL && traversed + l.span as usize <= target {
                    traversed += l.span as usize;
                    x = l.forward;
                } else {
                    break;
                }
            }
            if traversed == target && x != HEAD {
                return Some(x);
            }
        }
        None
    }

    // ---- score ranges ---------------------------------------------------

    /// `zslIsInRange`.
    pub fn is_in_range(&self, r: &ScoreRange) -> bool {
        if r.is_empty() || self.length == 0 {
            return false;
        }
        let last = self.tail;
        if last == NIL || !r.gte_min(self.node(last).score) {
            return false;
        }
        let first = self.lvl(HEAD, 0).forward;
        first != NIL && r.lte_max(self.node(first).score)
    }

    /// `zslFirstInRange`, as a 0-based rank.
    pub fn first_in_range(&self, r: &ScoreRange) -> Option<usize> {
        let idx = self.first_node_in_range(r)?;
        self.rank_of_node(idx)
    }

    /// `zslLastInRange`, as a 0-based rank.
    pub fn last_in_range(&self, r: &ScoreRange) -> Option<usize> {
        let idx = self.last_node_in_range(r)?;
        self.rank_of_node(idx)
    }

    fn first_node_in_range(&self, r: &ScoreRange) -> Option<u32> {
        if !self.is_in_range(r) {
            return None;
        }
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.lvl(x, i).forward;
                if f != NIL && !r.gte_min(self.node(f).score) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        let x = self.lvl(x, 0).forward;
        if x != NIL && r.lte_max(self.node(x).score) {
            Some(x)
        } else {
            None
        }
    }

    fn last_node_in_range(&self, r: &ScoreRange) -> Option<u32> {
        if !self.is_in_range(r) {
            return None;
        }
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.lvl(x, i).forward;
                if f != NIL && r.lte_max(self.node(f).score) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        if x != HEAD && r.gte_min(self.node(x).score) {
            Some(x)
        } else {
            None
        }
    }

    // ---- lex ranges -----------------------------------------------------

    /// `zslIsInLexRange`.
    pub fn is_in_lex_range(&self, r: &LexRange) -> bool {
        if r.is_empty() || self.length == 0 {
            return false;
        }
        let last = self.tail;
        if last == NIL || !r.gte_min(self.node(last).member.as_ref()) {
            return false;
        }
        let first = self.lvl(HEAD, 0).forward;
        first != NIL && r.lte_max(self.node(first).member.as_ref())
    }

    /// `zslFirstInLexRange`, as a 0-based rank.
    pub fn first_in_lex_range(&self, r: &LexRange) -> Option<usize> {
        let idx = self.first_node_in_lex_range(r)?;
        self.rank_of_node(idx)
    }

    /// `zslLastInLexRange`, as a 0-based rank.
    pub fn last_in_lex_range(&self, r: &LexRange) -> Option<usize> {
        let idx = self.last_node_in_lex_range(r)?;
        self.rank_of_node(idx)
    }

    fn first_node_in_lex_range(&self, r: &LexRange) -> Option<u32> {
        if !self.is_in_lex_range(r) {
            return None;
        }
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.lvl(x, i).forward;
                if f != NIL && !r.gte_min(self.node(f).member.as_ref()) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        let x = self.lvl(x, 0).forward;
        if x != NIL && r.lte_max(self.node(x).member.as_ref()) {
            Some(x)
        } else {
            None
        }
    }

    fn last_node_in_lex_range(&self, r: &LexRange) -> Option<u32> {
        if !self.is_in_lex_range(r) {
            return None;
        }
        let mut x = HEAD;
        for i in (0..self.level).rev() {
            loop {
                let f = self.lvl(x, i).forward;
                if f != NIL && r.lte_max(self.node(f).member.as_ref()) {
                    x = f;
                } else {
                    break;
                }
            }
        }
        if x != HEAD && r.gte_min(self.node(x).member.as_ref()) {
            Some(x)
        } else {
            None
        }
    }

    /// 0-based rank of an arena node, by re-walking from the header.
    fn rank_of_node(&self, target: u32) -> Option<usize> {
        let n = self.node(target);
        self.rank_of(n.score, n.member.as_ref())
    }

    // ---- bulk delete ----------------------------------------------------

    /// `zslDeleteRangeByScore`. `on_delete` sees every removed member so the
    /// paired dict can be kept in step; it is called before the node is
    /// released.
    pub fn delete_range_by_score(
        &mut self,
        r: &ScoreRange,
        mut on_delete: impl FnMut(&Bytes, f64),
    ) -> usize {
        self.delete_while(
            |sl| sl.first_node_in_range(r),
            |sl, x| r.lte_max(sl.node(x).score),
            &mut on_delete,
        )
    }

    /// `zslDeleteRangeByLex`.
    pub fn delete_range_by_lex(
        &mut self,
        r: &LexRange,
        mut on_delete: impl FnMut(&Bytes, f64),
    ) -> usize {
        self.delete_while(
            |sl| sl.first_node_in_lex_range(r),
            |sl, x| r.lte_max(sl.node(x).member.as_ref()),
            &mut on_delete,
        )
    }

    /// `zslDeleteRangeByRank`, 0-based and inclusive of `start`, removing at
    /// most `count` elements.
    pub fn delete_range_by_rank(
        &mut self,
        start: usize,
        count: usize,
        mut on_delete: impl FnMut(&Bytes, f64),
    ) -> usize {
        if count == 0 || start >= self.length {
            return 0;
        }
        let mut removed = 0usize;
        while removed < count {
            let Some(x) = self.node_by_rank(start) else {
                break;
            };
            let (m, s) = {
                let n = self.node(x);
                (n.member.clone(), n.score)
            };
            on_delete(&m, s);
            let mut update = [HEAD; MAX_LEVEL];
            if self.find_with_update(s, m.as_ref(), &mut update).is_none() {
                break;
            }
            self.unlink(x, &update);
            self.release(x);
            removed += 1;
        }
        removed
    }

    fn delete_while(
        &mut self,
        first: impl Fn(&Self) -> Option<u32>,
        still_in: impl Fn(&Self, u32) -> bool,
        on_delete: &mut impl FnMut(&Bytes, f64),
    ) -> usize {
        let mut removed = 0usize;
        while let Some(x) = first(self) {
            if !still_in(self, x) {
                break;
            }
            let (m, s) = {
                let n = self.node(x);
                (n.member.clone(), n.score)
            };
            on_delete(&m, s);
            let mut update = [HEAD; MAX_LEVEL];
            if self.find_with_update(s, m.as_ref(), &mut update).is_none() {
                break;
            }
            self.unlink(x, &update);
            self.release(x);
            removed += 1;
        }
        removed
    }

    // ---- iteration -------------------------------------------------------

    /// Lowest-ranked element.
    #[inline]
    pub fn first(&self) -> Option<(&Bytes, f64)> {
        let x = self.lvl(HEAD, 0).forward;
        if x == NIL {
            return None;
        }
        let n = self.node(x);
        Some((&n.member, n.score))
    }

    /// Highest-ranked element.
    #[inline]
    pub fn last(&self) -> Option<(&Bytes, f64)> {
        if self.tail == NIL {
            return None;
        }
        let n = self.node(self.tail);
        Some((&n.member, n.score))
    }

    /// Ascending iterator over the whole list.
    #[inline]
    pub fn iter(&self) -> SkiplistIter<'_> {
        SkiplistIter {
            sl: self,
            cur: self.lvl(HEAD, 0).forward,
            remaining: self.length,
        }
    }

    /// Descending iterator over the whole list.
    #[inline]
    pub fn iter_rev(&self) -> SkiplistRevIter<'_> {
        SkiplistRevIter {
            sl: self,
            cur: self.tail,
            remaining: self.length,
        }
    }

    /// Ascending iterator starting at 0-based `rank`.
    #[inline]
    pub fn iter_from_rank(&self, rank: usize) -> SkiplistIter<'_> {
        match self.node_by_rank(rank) {
            Some(x) => SkiplistIter {
                sl: self,
                cur: x,
                remaining: self.length - rank,
            },
            None => SkiplistIter {
                sl: self,
                cur: NIL,
                remaining: 0,
            },
        }
    }

    /// Descending iterator starting at 0-based `rank` and walking down.
    #[inline]
    pub fn iter_rev_from_rank(&self, rank: usize) -> SkiplistRevIter<'_> {
        match self.node_by_rank(rank) {
            Some(x) => SkiplistRevIter {
                sl: self,
                cur: x,
                remaining: rank + 1,
            },
            None => SkiplistRevIter {
                sl: self,
                cur: NIL,
                remaining: 0,
            },
        }
    }

    /// Ascending iterator over everything in a score range.
    pub fn range(&self, r: &ScoreRange) -> SkiplistIter<'_> {
        match self.first_node_in_range(r) {
            Some(x) => SkiplistIter {
                sl: self,
                cur: x,
                remaining: self.length,
            }
            .stop_at_score_max(r),
            None => SkiplistIter {
                sl: self,
                cur: NIL,
                remaining: 0,
            },
        }
    }

    /// Ascending iterator over everything in a lexicographic range.
    pub fn lex_range(&self, r: &LexRange) -> SkiplistIter<'_> {
        match self.first_node_in_lex_range(r) {
            Some(x) => {
                let last = self.last_node_in_lex_range(r);
                let n = match (
                    self.rank_of_node(x),
                    last.and_then(|l| self.rank_of_node(l)),
                ) {
                    (Some(a), Some(b)) if b >= a => b - a + 1,
                    _ => 0,
                };
                SkiplistIter {
                    sl: self,
                    cur: x,
                    remaining: n,
                }
            }
            None => SkiplistIter {
                sl: self,
                cur: NIL,
                remaining: 0,
            },
        }
    }
}

/// Ascending iterator. Yields `(member, score)` borrowed from the arena.
#[derive(Clone)]
pub struct SkiplistIter<'a> {
    sl: &'a Skiplist,
    cur: u32,
    remaining: usize,
}

impl<'a> SkiplistIter<'a> {
    fn stop_at_score_max(mut self, r: &ScoreRange) -> Self {
        // Convert the score bound into a count so `next` stays branch-light.
        let n = match (
            self.sl.rank_of_node(self.cur),
            self.sl
                .last_node_in_range(r)
                .and_then(|l| self.sl.rank_of_node(l)),
        ) {
            (Some(a), Some(b)) if b >= a => b - a + 1,
            _ => 0,
        };
        self.remaining = n;
        self
    }
}

impl<'a> Iterator for SkiplistIter<'a> {
    type Item = (&'a Bytes, f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur == NIL || self.remaining == 0 {
            return None;
        }
        let n = self.sl.node(self.cur);
        self.cur = n.levels.first().map_or(NIL, |l| l.forward);
        self.remaining -= 1;
        Some((&n.member, n.score))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// Descending iterator.
#[derive(Clone)]
pub struct SkiplistRevIter<'a> {
    sl: &'a Skiplist,
    cur: u32,
    remaining: usize,
}

impl<'a> Iterator for SkiplistRevIter<'a> {
    type Item = (&'a Bytes, f64);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.cur == NIL || self.remaining == 0 {
            return None;
        }
        let n = self.sl.node(self.cur);
        self.cur = n.backward;
        self.remaining -= 1;
        Some((&n.member, n.score))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    fn dump(sl: &Skiplist) -> Vec<(String, f64)> {
        sl.iter()
            .map(|(m, s)| (String::from_utf8_lossy(m).into_owned(), s))
            .collect()
    }

    #[test]
    fn empty() {
        let mut sl = Skiplist::new();
        assert_eq!(sl.len(), 0);
        assert!(sl.is_empty());
        assert!(sl.first().is_none() && sl.last().is_none());
        assert_eq!(sl.by_rank(0), None);
        assert_eq!(sl.rank_of(1.0, b"a"), None);
        assert!(!sl.delete(1.0, b"a"));
        assert_eq!(sl.iter().count(), 0);
        assert_eq!(sl.iter_rev().count(), 0);
        assert!(!sl.is_in_range(&ScoreRange::all()));
    }

    #[test]
    fn single_element() {
        let mut sl = Skiplist::new();
        sl.insert(1.5, b("only"));
        assert_eq!(sl.len(), 1);
        assert_eq!(sl.rank_of(1.5, b"only"), Some(0));
        assert_eq!(
            sl.by_rank(0).map(|(m, s)| (m.clone(), s)),
            Some((b("only"), 1.5))
        );
        assert_eq!(sl.by_rank(1), None);
        assert!(sl.delete(1.5, b"only"));
        assert!(sl.is_empty());
        assert_eq!(sl.level(), 1);
    }

    #[test]
    fn ordering_is_score_then_memcmp() {
        let mut sl = Skiplist::new();
        for m in ["c", "a", "b"] {
            sl.insert(1.0, b(m));
        }
        sl.insert(0.5, b("zzz"));
        sl.insert(2.0, b("aaa"));
        assert_eq!(
            dump(&sl),
            vec![
                ("zzz".into(), 0.5),
                ("a".into(), 1.0),
                ("b".into(), 1.0),
                ("c".into(), 1.0),
                ("aaa".into(), 2.0),
            ]
        );
        // memcmp then length, exactly like sdscmp.
        let mut sl = Skiplist::new();
        for m in ["ab", "a", "b", ""] {
            sl.insert(0.0, b(m));
        }
        assert_eq!(
            dump(&sl).into_iter().map(|(m, _)| m).collect::<Vec<_>>(),
            vec!["", "a", "ab", "b"]
        );
    }

    #[test]
    fn ranks_are_dense_and_zero_based() {
        let mut sl = Skiplist::new();
        for i in 0..200 {
            sl.insert(f64::from(i), b(&format!("m{i:04}")));
        }
        for i in 0..200usize {
            let (m, s) = sl.by_rank(i).expect("rank in range");
            assert_eq!(s, i as f64);
            assert_eq!(sl.rank_of(s, m.as_ref()), Some(i));
        }
        assert_eq!(sl.by_rank(200), None);
        assert_eq!(sl.rank_of(1.0, b"nope"), None);
        assert_eq!(sl.rank_of(999.0, b"m0000"), None);
    }

    #[test]
    fn reverse_iteration_and_rank_starts() {
        let mut sl = Skiplist::new();
        for i in 0..10 {
            sl.insert(f64::from(i), b(&format!("m{i}")));
        }
        let fwd: Vec<_> = sl.iter().map(|(_, s)| s).collect();
        let mut rev: Vec<_> = sl.iter_rev().map(|(_, s)| s).collect();
        rev.reverse();
        assert_eq!(fwd, rev);
        assert_eq!(
            sl.iter_from_rank(7).map(|(_, s)| s).collect::<Vec<_>>(),
            vec![7.0, 8.0, 9.0]
        );
        assert_eq!(
            sl.iter_rev_from_rank(2).map(|(_, s)| s).collect::<Vec<_>>(),
            vec![2.0, 1.0, 0.0]
        );
        assert_eq!(sl.iter_from_rank(10).count(), 0);
    }

    #[test]
    fn deleting_the_tail_repairs_the_backward_chain() {
        let mut sl = Skiplist::new();
        for i in 0..50 {
            sl.insert(f64::from(i), b(&format!("m{i:02}")));
        }
        assert!(sl.delete(49.0, b"m49"));
        assert_eq!(sl.last().map(|(_, s)| s), Some(48.0));
        let mut rev: Vec<_> = sl.iter_rev().map(|(_, s)| s).collect();
        rev.reverse();
        assert_eq!(rev, sl.iter().map(|(_, s)| s).collect::<Vec<_>>());
        // And the head.
        assert!(sl.delete(0.0, b"m00"));
        assert_eq!(sl.first().map(|(_, s)| s), Some(1.0));
        assert_eq!(sl.len(), 48);
        for (i, (_, s)) in sl.iter().enumerate() {
            assert_eq!(s, (i + 1) as f64);
        }
    }

    #[test]
    fn update_score_keeps_order() {
        let mut sl = Skiplist::new();
        for i in 0..10 {
            sl.insert(f64::from(i), b(&format!("m{i}")));
        }
        // In-place: still between its neighbours.
        assert!(sl.update_score(5.0, b"m5", 5.5));
        assert_eq!(sl.rank_of(5.5, b"m5"), Some(5));
        // Reordering: move to the front.
        assert!(sl.update_score(5.5, b"m5", -100.0));
        assert_eq!(sl.rank_of(-100.0, b"m5"), Some(0));
        // And to the back.
        assert!(sl.update_score(-100.0, b"m5", 100.0));
        assert_eq!(sl.rank_of(100.0, b"m5"), Some(9));
        assert!(!sl.update_score(1.0, b"missing", 2.0));
        assert_eq!(sl.len(), 10);
    }

    #[test]
    fn score_ranges_with_exclusive_bounds_and_infinities() {
        let mut sl = Skiplist::new();
        for i in 1..=5 {
            sl.insert(f64::from(i), b(&format!("m{i}")));
        }
        let all = ScoreRange::all();
        assert_eq!(sl.first_in_range(&all), Some(0));
        assert_eq!(sl.last_in_range(&all), Some(4));
        assert_eq!(sl.range(&all).count(), 5);

        let r = ScoreRange::new(ScoreBound::incl(2.0), ScoreBound::incl(4.0));
        assert_eq!(
            sl.range(&r).map(|(_, s)| s).collect::<Vec<_>>(),
            vec![2.0, 3.0, 4.0]
        );

        let r = ScoreRange::new(ScoreBound::excl(2.0), ScoreBound::excl(4.0));
        assert_eq!(sl.range(&r).map(|(_, s)| s).collect::<Vec<_>>(), vec![3.0]);

        let r = ScoreRange::new(ScoreBound::incl(10.0), ScoreBound::incl(20.0));
        assert_eq!(sl.first_in_range(&r), None);
        assert_eq!(sl.range(&r).count(), 0);

        let r = ScoreRange::new(ScoreBound::incl(f64::NEG_INFINITY), ScoreBound::excl(1.0));
        assert_eq!(sl.range(&r).count(), 0);

        // An empty spec is empty regardless of contents.
        assert!(ScoreRange::new(ScoreBound::incl(5.0), ScoreBound::incl(1.0)).is_empty());
        assert!(ScoreRange::new(ScoreBound::excl(1.0), ScoreBound::incl(1.0)).is_empty());
        assert!(!ScoreRange::new(ScoreBound::incl(1.0), ScoreBound::incl(1.0)).is_empty());
    }

    #[test]
    fn lex_ranges() {
        let mut sl = Skiplist::new();
        for m in ["a", "b", "c", "d", "e"] {
            sl.insert(0.0, b(m));
        }
        let names = |it: SkiplistIter<'_>| {
            it.map(|(m, _)| String::from_utf8_lossy(m).into_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            names(sl.lex_range(&LexRange::all())),
            vec!["a", "b", "c", "d", "e"]
        );
        assert_eq!(
            names(sl.lex_range(&LexRange::new(
                LexBound::Incl(b("b")),
                LexBound::Incl(b("d"))
            ))),
            vec!["b", "c", "d"]
        );
        assert_eq!(
            names(sl.lex_range(&LexRange::new(
                LexBound::Excl(b("b")),
                LexBound::Excl(b("d"))
            ))),
            vec!["c"]
        );
        assert_eq!(
            names(sl.lex_range(&LexRange::new(LexBound::NegInf, LexBound::Excl(b("a"))))),
            Vec::<String>::new()
        );
        assert_eq!(
            names(sl.lex_range(&LexRange::new(LexBound::Excl(b("e")), LexBound::PosInf))),
            Vec::<String>::new()
        );
        assert_eq!(sl.first_in_lex_range(&LexRange::all()), Some(0));
        assert_eq!(sl.last_in_lex_range(&LexRange::all()), Some(4));
        assert!(LexRange::new(LexBound::PosInf, LexBound::PosInf).is_empty());
        assert!(LexRange::new(LexBound::Incl(b("z")), LexBound::Incl(b("a"))).is_empty());
        assert!(!LexRange::new(LexBound::Incl(b("a")), LexBound::Incl(b("a"))).is_empty());
        assert!(LexRange::new(LexBound::Excl(b("a")), LexBound::Incl(b("a"))).is_empty());
    }

    #[test]
    fn bulk_deletes() {
        let mut sl = Skiplist::new();
        for i in 0..20 {
            sl.insert(f64::from(i), b(&format!("m{i:02}")));
        }
        let mut seen = Vec::new();
        let n = sl.delete_range_by_score(
            &ScoreRange::new(ScoreBound::incl(5.0), ScoreBound::incl(9.0)),
            |m, _| seen.push(String::from_utf8_lossy(m).into_owned()),
        );
        assert_eq!(n, 5);
        assert_eq!(seen, vec!["m05", "m06", "m07", "m08", "m09"]);
        assert_eq!(sl.len(), 15);

        let n = sl.delete_range_by_rank(0, 3, |_, _| {});
        assert_eq!(n, 3);
        assert_eq!(sl.first().map(|(_, s)| s), Some(3.0));
        assert_eq!(sl.len(), 12);

        let n = sl.delete_range_by_rank(100, 3, |_, _| {});
        assert_eq!(n, 0);

        let mut sl = Skiplist::new();
        for m in ["a", "b", "c", "d"] {
            sl.insert(0.0, b(m));
        }
        let n = sl.delete_range_by_lex(
            &LexRange::new(LexBound::Incl(b("b")), LexBound::Incl(b("c"))),
            |_, _| {},
        );
        assert_eq!(n, 2);
        assert_eq!(
            dump(&sl).into_iter().map(|(m, _)| m).collect::<Vec<_>>(),
            vec!["a", "d"]
        );
    }

    #[test]
    fn arena_slots_are_reused() {
        let mut sl = Skiplist::new();
        for i in 0..100 {
            sl.insert(f64::from(i), b(&format!("m{i:03}")));
        }
        let peak = sl.nodes.len();
        for i in 0..100 {
            sl.delete(f64::from(i), format!("m{i:03}").as_bytes());
        }
        assert!(sl.is_empty());
        for i in 0..100 {
            sl.insert(f64::from(i), b(&format!("n{i:03}")));
        }
        assert_eq!(sl.nodes.len(), peak, "free list should have been reused");
        assert_eq!(sl.len(), 100);
    }

    #[test]
    fn clear_resets_everything() {
        let mut sl = Skiplist::new();
        for i in 0..64 {
            sl.insert(f64::from(i), b(&format!("m{i}")));
        }
        sl.clear();
        assert!(sl.is_empty());
        assert_eq!(sl.level(), 1);
        assert!(sl.first().is_none() && sl.last().is_none());
        sl.insert(1.0, b("x"));
        assert_eq!(dump(&sl), vec![("x".into(), 1.0)]);
    }

    #[test]
    fn infinities_sort_correctly() {
        let mut sl = Skiplist::new();
        sl.insert(f64::NEG_INFINITY, b("lo"));
        sl.insert(0.0, b("mid"));
        sl.insert(f64::INFINITY, b("hi"));
        assert_eq!(
            dump(&sl).into_iter().map(|(m, _)| m).collect::<Vec<_>>(),
            vec!["lo", "mid", "hi"]
        );
        assert_eq!(sl.rank_of(f64::INFINITY, b"hi"), Some(2));
        let r = ScoreRange::new(
            ScoreBound::excl(f64::NEG_INFINITY),
            ScoreBound::incl(f64::INFINITY),
        );
        assert_eq!(sl.range(&r).count(), 2);
    }
}
