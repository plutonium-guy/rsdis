//! Rax -- the compressed radix tree behind streams and cluster metadata.
//!
//! Owned by W1a; do not edit if you are not that agent.
//!
//! Keys are big-endian 128-bit stream IDs, so lexicographic order is numeric
//! order and range scans are prefix walks. `XRANGE`, `XREVRANGE` and `XREAD`
//! are all built on [`Rax::seek`] with `>=`, `>`, `<=` and `<` semantics, so
//! those four are first-class here rather than an afterthought.
//!
//! # Compressed nodes
//!
//! The point of a rax over a plain trie is that a run of single-child nodes
//! collapses into one node holding the shared substring. Storing 1000 stream
//! IDs that share a 12-byte millisecond prefix costs one 12-byte edge, not
//! twelve nodes.
//!
//! Node model, matching `rax.c`:
//!
//! * A node's *position* is the concatenation of the edge labels on the path
//!   from the root to it. `value` is the value of the key at that position.
//! * `chars` are the labels of the edges to this node's **children**.
//!   - branching (`compressed == false`): `chars.len() == children.len()`,
//!     each `chars[i]` is a one-byte edge to `children[i]`, sorted ascending.
//!   - compressed (`compressed == true`): `children.len() == 1` and `chars` is
//!     the multi-byte label of the single edge. Normalised so a one-byte label
//!     is always stored branching, never compressed.
//!
//! ```text
//!   ["foo"]  <- position "",  compressed, edge "foo"
//!      |
//!   [t   b]  <- position "foo", branching, edges 't' and 'b'
//!   /     \
//! ("er")  ("ar")
//!   |        |
//!  []       []   <- positions "footer" and "foobar", both keys
//! ```
//!
//! # Representation
//!
//! An arena (`Vec<Node>`) with `u32` indices, plus a free list. `rax.c` packs
//! each node into one variable-size allocation and walks it with pointer
//! arithmetic; the arena gets the same locality with **no `unsafe` at all**,
//! which is worth more here than shaving a few bytes per node. `unsafe` was
//! permitted for this module (§6) and was not needed.
//!
//! Iteration keeps an explicit path stack, exactly as `raxIterator` does, so
//! nodes need no parent pointers and splits never have to fix them up.

use smallvec::SmallVec;

/// The root's arena index. Its position is the empty key.
const ROOT: u32 = 0;

/// Seek relation for [`Rax::seek`] / [`Rax::seek_rev`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seek {
    /// First key >= the given one.
    Ge,
    /// First key > the given one.
    Gt,
    /// Last key <= the given one.
    Le,
    /// Last key < the given one.
    Lt,
}

#[derive(Debug, Clone)]
struct Node<V> {
    /// Edge labels to the children (see the module header).
    chars: SmallVec<[u8; 8]>,
    children: SmallVec<[u32; 2]>,
    compressed: bool,
    value: Option<V>,
}

impl<V> Node<V> {
    #[inline]
    fn leaf() -> Self {
        Node {
            chars: SmallVec::new(),
            children: SmallVec::new(),
            compressed: false,
            value: None,
        }
    }
}

/// One step on the path from the root to the current node.
#[derive(Debug, Clone, Copy)]
struct Frame {
    /// The node we descended *from*.
    node: u32,
    /// Which child index we took.
    child: u32,
    /// Key length before that step's characters were appended.
    klen: u32,
}

/// A compressed radix tree mapping byte strings to `V`.
#[derive(Debug, Clone)]
pub struct Rax<V> {
    nodes: Vec<Node<V>>,
    free: Vec<u32>,
    numele: usize,
}

impl<V> Default for Rax<V> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<V> Rax<V> {
    /// `raxNew`.
    #[inline]
    pub fn new() -> Self {
        Rax {
            nodes: vec![Node::leaf()],
            free: Vec::new(),
            numele: 0,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.numele
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.numele == 0
    }

    /// Live arena nodes, for tests that assert compression actually happened.
    #[inline]
    pub fn node_count(&self) -> usize {
        self.nodes.len() - self.free.len()
    }

    /// Approximate heap footprint, for `MEMORY USAGE`.
    pub fn mem_usage(&self) -> usize {
        core::mem::size_of::<Self>()
            + self.nodes.capacity() * core::mem::size_of::<Node<V>>()
            + self.free.capacity() * 4
            + self
                .nodes
                .iter()
                .map(|n| {
                    let c = if n.chars.spilled() {
                        n.chars.capacity()
                    } else {
                        0
                    };
                    let k = if n.children.spilled() {
                        n.children.capacity() * 4
                    } else {
                        0
                    };
                    c + k
                })
                .sum::<usize>()
    }

    #[inline]
    fn node(&self, i: u32) -> &Node<V> {
        &self.nodes[i as usize]
    }

    #[inline]
    fn node_mut(&mut self, i: u32) -> &mut Node<V> {
        &mut self.nodes[i as usize]
    }

    fn alloc(&mut self, n: Node<V>) -> u32 {
        match self.free.pop() {
            Some(i) => {
                self.nodes[i as usize] = n;
                i
            }
            None => {
                let i = self.nodes.len() as u32;
                self.nodes.push(n);
                i
            }
        }
    }

    fn release(&mut self, i: u32) {
        self.nodes[i as usize] = Node::leaf();
        self.free.push(i);
    }

    /// A node holding `label` as the single edge to `child`. Normalises a
    /// one-byte label to a branching node.
    fn labelled(&mut self, label: &[u8], child: u32) -> u32 {
        let compressed = label.len() > 1;
        let n = Node {
            chars: SmallVec::from_slice(label),
            children: SmallVec::from_slice(&[child]),
            compressed,
            value: None,
        };
        self.alloc(n)
    }

    // ---- lookup ----------------------------------------------------------

    /// Walk as far down as `key` allows.
    ///
    /// Returns `(node, consumed, split)` where `node` sits at position
    /// `key[..consumed]` when `split == 0`, and otherwise `node` is a
    /// compressed node whose label matched `split` bytes before diverging.
    fn walk(
        &self,
        key: &[u8],
        mut stack: Option<&mut SmallVec<[Frame; 16]>>,
    ) -> (u32, usize, usize) {
        let mut cur = ROOT;
        let mut i = 0usize;
        loop {
            let n = self.node(cur);
            if n.children.is_empty() || i == key.len() {
                return (cur, i, 0);
            }
            if n.compressed {
                let lab = n.chars.as_slice();
                let mut j = 0usize;
                while j < lab.len() && i + j < key.len() && lab[j] == key[i + j] {
                    j += 1;
                }
                if j != lab.len() {
                    return (cur, i + j, j);
                }
                if let Some(s) = stack.as_deref_mut() {
                    s.push(Frame {
                        node: cur,
                        child: 0,
                        klen: i as u32,
                    });
                }
                i += lab.len();
                cur = n.children[0];
            } else {
                let Ok(j) = n.chars.binary_search(&key[i]) else {
                    return (cur, i, 0);
                };
                if let Some(s) = stack.as_deref_mut() {
                    s.push(Frame {
                        node: cur,
                        child: j as u32,
                        klen: i as u32,
                    });
                }
                i += 1;
                cur = n.children[j];
            }
        }
    }

    /// `raxFind`.
    pub fn find(&self, key: &[u8]) -> Option<&V> {
        let (n, consumed, split) = self.walk(key, None);
        if consumed != key.len() || split != 0 {
            return None;
        }
        self.node(n).value.as_ref()
    }

    /// Mutable `raxFind`.
    pub fn find_mut(&mut self, key: &[u8]) -> Option<&mut V> {
        let (n, consumed, split) = self.walk(key, None);
        if consumed != key.len() || split != 0 {
            return None;
        }
        self.node_mut(n).value.as_mut()
    }

    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.find(key).is_some()
    }

    // ---- insert ----------------------------------------------------------

    /// `raxInsert`. Returns the previous value when the key was present.
    pub fn insert(&mut self, key: &[u8], value: V) -> Option<V> {
        let mut cur = ROOT;
        let mut i = 0usize;

        loop {
            if i == key.len() {
                break;
            }
            if self.node(cur).children.is_empty() {
                break;
            }
            if self.node(cur).compressed {
                let m = {
                    let lab = self.node(cur).chars.as_slice();
                    let mut m = 0usize;
                    while m < lab.len() && i + m < key.len() && lab[m] == key[i + m] {
                        m += 1;
                    }
                    m
                };
                if m == self.node(cur).chars.len() {
                    i += m;
                    cur = self.node(cur).children[0];
                    continue;
                }
                // Diverged inside the label: split so that `cur` becomes a
                // branching node sitting exactly at position key[..i + m].
                i += m;
                cur = self.split_compressed(cur, m);
                break;
            }
            let Ok(j) = self.node(cur).chars.binary_search(&key[i]) else {
                break;
            };
            i += 1;
            cur = self.node(cur).children[j];
        }

        if i == key.len() {
            let old = self.node_mut(cur).value.replace(value);
            if old.is_none() {
                self.numele += 1;
            }
            return old;
        }

        // Extend with the rest of the key. `cur` is either a leaf or a
        // branching node with no edge for `key[i]`.
        let rest = &key[i..];
        let leaf = self.alloc(Node::leaf());
        if self.node(cur).children.is_empty() && rest.len() > 1 {
            // A leaf can absorb the whole remainder as one compressed edge.
            let n = self.node_mut(cur);
            n.chars = SmallVec::from_slice(rest);
            n.children = SmallVec::from_slice(&[leaf]);
            n.compressed = true;
        } else {
            let first = rest[0];
            let child = if rest.len() > 1 {
                self.labelled(&rest[1..], leaf)
            } else {
                leaf
            };
            let n = self.node_mut(cur);
            let at = n.chars.partition_point(|&c| c < first);
            n.chars.insert(at, first);
            n.children.insert(at, child);
            n.compressed = false;
        }
        self.node_mut(leaf).value = Some(value);
        self.numele += 1;
        None
    }

    /// Split the compressed node `node` after `m` label bytes.
    ///
    /// Returns the index of the **branching** node that ends up at position
    /// `position(node) + label[..m]`, whose single existing edge is
    /// `label[m]`. `m` must be less than the label length.
    fn split_compressed(&mut self, node: u32, m: usize) -> u32 {
        let label: SmallVec<[u8; 8]> = self.node(node).chars.clone();
        let child = self.node(node).children[0];

        // Everything after the divergence point hangs off the split point.
        let tail = if m + 1 < label.len() {
            self.labelled(&label[m + 1..], child)
        } else {
            child
        };

        if m == 0 {
            // The divergence is at `node` itself: it becomes branching.
            let n = self.node_mut(node);
            n.chars = SmallVec::from_slice(&label[..1]);
            n.children = SmallVec::from_slice(&[tail]);
            n.compressed = false;
            node
        } else {
            let mid = self.alloc(Node {
                chars: SmallVec::from_slice(&label[m..m + 1]),
                children: SmallVec::from_slice(&[tail]),
                compressed: false,
                value: None,
            });
            let n = self.node_mut(node);
            n.chars = SmallVec::from_slice(&label[..m]);
            n.children = SmallVec::from_slice(&[mid]);
            n.compressed = m > 1;
            mid
        }
    }

    // ---- remove ----------------------------------------------------------

    /// `raxRemove`. Returns the removed value.
    pub fn remove(&mut self, key: &[u8]) -> Option<V> {
        let mut stack: SmallVec<[Frame; 16]> = SmallVec::new();
        let (cur, consumed, split) = self.walk(key, Some(&mut stack));
        if consumed != key.len() || split != 0 {
            return None;
        }
        let old = self.node_mut(cur).value.take()?;
        self.numele -= 1;

        // Prune the now-dead leaf chain, then re-compress what is left.
        let mut node = cur;
        while node != ROOT && self.node(node).children.is_empty() && self.node(node).value.is_none()
        {
            let Some(f) = stack.pop() else { break };
            let parent = f.node;
            let idx = f.child as usize;
            let p = self.node_mut(parent);
            p.chars.remove(idx);
            p.children.remove(idx);
            if p.compressed {
                // A compressed node has exactly one edge; losing it makes the
                // node a leaf, and the whole label goes with it.
                p.chars.clear();
                p.compressed = false;
            }
            self.release(node);
            node = parent;
        }

        // Walk back up merging single-child chains (rax.c's re-compression).
        self.try_merge(node);
        while let Some(f) = stack.pop() {
            self.try_merge(f.node);
        }
        Some(old)
    }

    /// Fold `node` and its only child into one compressed node, when the child
    /// is not itself a key and not a branch point.
    fn try_merge(&mut self, node: u32) {
        loop {
            if self.node(node).children.len() != 1 {
                return;
            }
            let child = self.node(node).children[0];
            if self.node(child).value.is_some() || self.node(child).children.len() != 1 {
                return;
            }
            let (clabel, cchild) = {
                let c = self.node(child);
                (c.chars.clone(), c.children[0])
            };
            let n = self.node_mut(node);
            n.chars.extend_from_slice(&clabel);
            n.children[0] = cchild;
            n.compressed = n.chars.len() > 1;
            self.release(child);
        }
    }

    /// Drop every key, keeping the arena allocation.
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.nodes.push(Node::leaf());
        self.free.clear();
        self.numele = 0;
    }

    // ---- iteration -------------------------------------------------------

    /// Ascending iterator over every key.
    pub fn iter(&self) -> RaxIter<'_, V> {
        let mut it = RaxIter::new(self);
        it.start_first();
        it
    }

    /// Descending iterator over every key.
    pub fn iter_rev(&self) -> RaxIter<'_, V> {
        let mut it = RaxIter::new(self);
        it.start_last();
        it
    }

    /// Ascending iterator positioned by `op` relative to `key`.
    ///
    /// `Seek::Ge` / `Seek::Gt` are the natural forward starts; `Seek::Le` /
    /// `Seek::Lt` position on the predecessor and then walk *forward* from
    /// there, which is what `XREAD` wants when resuming after a known ID.
    pub fn seek(&self, op: Seek, key: &[u8]) -> RaxIter<'_, V> {
        let mut it = RaxIter::new(self);
        it.do_seek(op, key);
        it.forward = true;
        it
    }

    /// Descending iterator positioned by `op` relative to `key`. `XREVRANGE`
    /// is `seek_rev(Le, end)`.
    pub fn seek_rev(&self, op: Seek, key: &[u8]) -> RaxIter<'_, V> {
        let mut it = RaxIter::new(self);
        it.do_seek(op, key);
        it.forward = false;
        it
    }

    /// Smallest key and its value.
    pub fn first(&self) -> Option<(SmallVec<[u8; 32]>, &V)> {
        let mut it = self.iter();
        it.next().map(|(k, v)| (SmallVec::from_slice(k), v))
    }

    /// Largest key and its value.
    pub fn last(&self) -> Option<(SmallVec<[u8; 32]>, &V)> {
        let mut it = self.iter_rev();
        it.next().map(|(k, v)| (SmallVec::from_slice(k), v))
    }
}

/// A cursor over a [`Rax`], yielding `(&[u8], &V)`.
///
/// This is a *lending* iterator: the key borrows the cursor's own buffer, so
/// it cannot implement `std::iter::Iterator`. Use it as
/// `while let Some((k, v)) = it.next() { ... }`, which is exactly how
/// `raxNext` is used in `t_stream.c`.
pub struct RaxIter<'a, V> {
    rax: &'a Rax<V>,
    key: SmallVec<[u8; 64]>,
    stack: SmallVec<[Frame; 16]>,
    cur: u32,
    /// Set once positioned; the first `next()` returns the current element.
    pending: bool,
    eof: bool,
    forward: bool,
}

impl<'a, V> RaxIter<'a, V> {
    fn new(rax: &'a Rax<V>) -> Self {
        RaxIter {
            rax,
            key: SmallVec::new(),
            stack: SmallVec::new(),
            cur: ROOT,
            pending: false,
            eof: true,
            forward: true,
        }
    }

    /// The key the cursor is currently on. Empty when exhausted.
    #[inline]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Advance and yield. Direction follows how the cursor was created.
    #[allow(clippy::should_implement_trait)] // lending iterator; see the docs above
    pub fn next(&mut self) -> Option<(&[u8], &'a V)> {
        if self.eof {
            return None;
        }
        if self.pending {
            self.pending = false;
        } else {
            let ok = if self.forward {
                self.step_forward(true)
            } else {
                self.step_backward(false)
            };
            if !ok {
                self.eof = true;
                return None;
            }
        }
        let v = self.rax.node(self.cur).value.as_ref()?;
        Some((&self.key, v))
    }

    /// Collect the remaining `(key, value)` pairs. Allocates; use `next` on
    /// hot paths.
    pub fn collect_pairs(&mut self) -> Vec<(Vec<u8>, &'a V)> {
        let mut out = Vec::new();
        while let Some((k, v)) = self.next() {
            out.push((k.to_vec(), v));
        }
        out
    }

    // ---- movement --------------------------------------------------------

    fn descend(&mut self, i: usize) {
        let n = self.rax.node(self.cur);
        self.stack.push(Frame {
            node: self.cur,
            child: i as u32,
            klen: self.key.len() as u32,
        });
        if n.compressed {
            self.key.extend_from_slice(&n.chars);
        } else if let Some(&c) = n.chars.get(i) {
            self.key.push(c);
        }
        self.cur = n.children[i];
    }

    fn ascend(&mut self) -> Option<Frame> {
        let f = self.stack.pop()?;
        self.key.truncate(f.klen as usize);
        self.cur = f.node;
        Some(f)
    }

    fn descend_rightmost(&mut self) {
        loop {
            let last = self.rax.node(self.cur).children.len();
            if last == 0 {
                return;
            }
            self.descend(last - 1);
        }
    }

    /// Move to the next key. `descend_first == false` skips the current
    /// node's whole subtree (used after a seek that landed above the target).
    fn step_forward(&mut self, mut descend_first: bool) -> bool {
        loop {
            if descend_first && !self.rax.node(self.cur).children.is_empty() {
                self.descend(0);
            } else {
                let mut moved = false;
                while let Some(f) = self.ascend() {
                    let n = self.rax.node(self.cur);
                    if (f.child as usize) + 1 < n.children.len() {
                        self.descend(f.child as usize + 1);
                        moved = true;
                        break;
                    }
                }
                if !moved {
                    return false;
                }
            }
            descend_first = true;
            if self.rax.node(self.cur).value.is_some() {
                return true;
            }
        }
    }

    /// Move to the previous key. `check_self` considers the current node
    /// itself first (used when a seek landed on a node that is a valid answer).
    fn step_backward(&mut self, check_self: bool) -> bool {
        if check_self && self.rax.node(self.cur).value.is_some() {
            return true;
        }
        loop {
            let Some(f) = self.ascend() else { return false };
            if f.child > 0 {
                self.descend(f.child as usize - 1);
                self.descend_rightmost();
                if self.rax.node(self.cur).value.is_some() {
                    return true;
                }
                // Otherwise keep unwinding; the loop tries earlier siblings.
            } else if self.rax.node(self.cur).value.is_some() {
                // No earlier sibling: the parent's own key precedes the
                // subtree we just left.
                return true;
            }
        }
    }

    // ---- positioning -----------------------------------------------------

    fn reset(&mut self) {
        self.key.clear();
        self.stack.clear();
        self.cur = ROOT;
        self.pending = false;
        self.eof = false;
    }

    fn start_first(&mut self) {
        self.reset();
        self.forward = true;
        if self.rax.node(ROOT).value.is_some() {
            self.pending = true;
            return;
        }
        if self.step_forward(true) {
            self.pending = true;
        } else {
            self.eof = true;
        }
    }

    fn start_last(&mut self) {
        self.reset();
        self.forward = false;
        self.descend_rightmost();
        if self.step_backward(true) {
            self.pending = true;
        } else {
            self.eof = true;
        }
    }

    /// Position on the first key satisfying `op` relative to `target`.
    fn do_seek(&mut self, op: Seek, target: &[u8]) {
        self.reset();
        let forward = matches!(op, Seek::Ge | Seek::Gt);

        // Walk down as far as the target matches, recording the path.
        let mut i = 0usize;
        let outcome = loop {
            let n = self.rax.node(self.cur);
            if i == target.len() {
                break Outcome::AtTarget;
            }
            if n.children.is_empty() {
                // A proper prefix of the target: strictly less than it.
                break Outcome::Below;
            }
            if n.compressed {
                let lab = n.chars.as_slice();
                let mut m = 0usize;
                while m < lab.len() && i + m < target.len() && lab[m] == target[i + m] {
                    m += 1;
                }
                if m == lab.len() {
                    self.descend(0);
                    i += m;
                    continue;
                }
                // A compressed node has one child, so the whole subtree falls
                // on one side of the target. The target ending inside the
                // label also puts the subtree above, since those keys extend
                // the target.
                let above = i + m == target.len() || lab[m] > target[i + m];
                break Outcome::Diverge {
                    above: if above { Some(0) } else { None },
                    below: if above { None } else { Some(0) },
                };
            }
            match n.chars.binary_search(&target[i]) {
                Ok(j) => {
                    self.descend(j);
                    i += 1;
                }
                Err(j) => {
                    // Children `>= j` are entirely above the target, children
                    // `< j` entirely below it. Both sides can be non-empty,
                    // and a backward seek needs the one a forward seek does
                    // not.
                    break Outcome::Diverge {
                        above: (j < n.children.len()).then_some(j),
                        below: j.checked_sub(1),
                    };
                }
            }
        };

        // In every non-`AtTarget` outcome the cursor sits on a proper prefix
        // of the target, so its own key (if any) is strictly less than it.

        let found = match (forward, outcome) {
            // --- forward ---
            (true, Outcome::AtTarget) => {
                if op == Seek::Ge && self.rax.node(self.cur).value.is_some() {
                    true
                } else {
                    self.step_forward(true)
                }
            }
            (true, Outcome::Below) => self.step_forward(false),
            // The first key above the target is the leftmost one under the
            // first child that sits above it.
            (true, Outcome::Diverge { above: Some(j), .. }) => {
                self.descend(j);
                if self.rax.node(self.cur).value.is_some() {
                    true
                } else {
                    self.step_forward(true)
                }
            }
            // Nothing here is above the target: skip the whole subtree.
            (true, Outcome::Diverge { above: None, .. }) => self.step_forward(false),

            // --- backward ---
            (false, Outcome::AtTarget) => {
                if op == Seek::Le && self.rax.node(self.cur).value.is_some() {
                    true
                } else {
                    self.step_backward(false)
                }
            }
            // The cursor's own key is a proper prefix of the target, hence
            // less than it, hence a candidate.
            (false, Outcome::Below) => self.step_backward(true),
            // The last key below the target is the rightmost one under the
            // last child that sits below it.
            (false, Outcome::Diverge { below: Some(j), .. }) => {
                self.descend(j);
                self.descend_rightmost();
                self.step_backward(true)
            }
            (false, Outcome::Diverge { below: None, .. }) => self.step_backward(true),
        };

        if found {
            self.pending = true;
            self.eof = false;
        } else {
            self.eof = true;
        }
    }
}

/// Where the seek walk stopped, relative to the target key.
#[derive(Debug, Clone, Copy)]
enum Outcome {
    /// The cursor sits exactly at the target's position.
    AtTarget,
    /// The cursor is on a leaf whose position is a proper prefix of the
    /// target: less than it, with no subtree to consider.
    Below,
    /// The walk diverged at the cursor, whose position is a proper prefix of
    /// the target. `above` is the first child whose subtree is entirely
    /// greater than the target; `below` the last whose subtree is entirely
    /// less. At a branching node both can be present -- which is the whole
    /// reason this is one variant and not two.
    Diverge {
        above: Option<usize>,
        below: Option<usize>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(rax: &Rax<u32>) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut it = rax.iter();
        while let Some((k, _)) = it.next() {
            out.push(k.to_vec());
        }
        out
    }

    fn strs(rax: &Rax<u32>) -> Vec<String> {
        keys(rax)
            .into_iter()
            .map(|k| String::from_utf8_lossy(&k).into_owned())
            .collect()
    }

    fn build(items: &[&str]) -> Rax<u32> {
        let mut r = Rax::new();
        for (i, k) in items.iter().enumerate() {
            r.insert(k.as_bytes(), i as u32);
        }
        r
    }

    #[test]
    fn empty() {
        let mut r: Rax<u32> = Rax::new();
        assert_eq!(r.len(), 0);
        assert!(r.is_empty());
        assert!(r.find(b"").is_none());
        assert!(r.find(b"x").is_none());
        assert!(r.remove(b"x").is_none());
        assert!(r.first().is_none() && r.last().is_none());
        assert!(r.iter().next().is_none());
        assert!(r.iter_rev().next().is_none());
        assert!(r.seek(Seek::Ge, b"").next().is_none());
    }

    #[test]
    fn empty_key_is_a_real_key() {
        let mut r: Rax<u32> = Rax::new();
        assert_eq!(r.insert(b"", 1), None);
        assert_eq!(r.find(b""), Some(&1));
        assert_eq!(r.len(), 1);
        r.insert(b"a", 2);
        assert_eq!(strs(&r), vec!["", "a"]);
        assert_eq!(r.remove(b""), Some(1));
        assert_eq!(strs(&r), vec!["a"]);
    }

    #[test]
    fn insert_find_overwrite() {
        let mut r = Rax::new();
        assert_eq!(r.insert(b"foo", 1), None);
        assert_eq!(r.insert(b"foo", 2), Some(1));
        assert_eq!(r.find(b"foo"), Some(&2));
        assert_eq!(r.len(), 1);
        assert!(r.find(b"fo").is_none());
        assert!(r.find(b"food").is_none());
        *r.find_mut(b"foo").expect("present") = 9;
        assert_eq!(r.find(b"foo"), Some(&9));
    }

    #[test]
    fn compression_actually_happens() {
        let mut r: Rax<u32> = Rax::new();
        r.insert(b"foobar", 1);
        // root + one compressed edge "foobar" + leaf.
        assert_eq!(r.node_count(), 2);
        r.insert(b"footer", 2);
        // root->"foo", branch on b/t, "ar"/"er", two leaves.
        assert_eq!(strs(&r), vec!["foobar", "footer"]);
        assert_eq!(r.find(b"foobar"), Some(&1));
        assert_eq!(r.find(b"footer"), Some(&2));
        // A plain trie would need 7+ nodes for the shared prefix alone.
        assert!(r.node_count() <= 6, "not compressed: {}", r.node_count());
    }

    #[test]
    fn prefix_key_splits_the_label() {
        // rax.c's "annibale" / "anni" case.
        let mut r = Rax::new();
        r.insert(b"annibale", 1);
        r.insert(b"anni", 2);
        assert_eq!(r.find(b"annibale"), Some(&1));
        assert_eq!(r.find(b"anni"), Some(&2));
        assert!(r.find(b"ann").is_none());
        assert!(r.find(b"annib").is_none());
        assert_eq!(strs(&r), vec!["anni", "annibale"]);
        // And the other order.
        let mut r = Rax::new();
        r.insert(b"anni", 2);
        r.insert(b"annibale", 1);
        assert_eq!(strs(&r), vec!["anni", "annibale"]);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn divergence_mid_label() {
        let mut r = Rax::new();
        r.insert(b"romane", 1);
        r.insert(b"romanus", 2);
        r.insert(b"romulus", 3);
        r.insert(b"rubens", 4);
        r.insert(b"ruber", 5);
        r.insert(b"rubicon", 6);
        r.insert(b"rubicundus", 7);
        assert_eq!(
            strs(&r),
            vec![
                "romane",
                "romanus",
                "romulus",
                "rubens",
                "ruber",
                "rubicon",
                "rubicundus"
            ]
        );
        for (k, v) in [
            ("romane", 1u32),
            ("romanus", 2),
            ("romulus", 3),
            ("rubens", 4),
            ("ruber", 5),
            ("rubicon", 6),
            ("rubicundus", 7),
        ] {
            assert_eq!(r.find(k.as_bytes()), Some(&v), "{k}");
        }
        assert!(r.find(b"rom").is_none());
        assert!(r.find(b"rub").is_none());
        assert!(r.find(b"rubicundu").is_none());
    }

    #[test]
    fn removal_prunes_and_recompresses() {
        let mut r = Rax::new();
        r.insert(b"foobar", 1);
        r.insert(b"footer", 2);
        let before = r.node_count();
        assert_eq!(r.remove(b"footer"), Some(2));
        assert_eq!(strs(&r), vec!["foobar"]);
        assert_eq!(r.find(b"foobar"), Some(&1));
        assert!(
            r.node_count() < before,
            "should have re-compressed: {} -> {}",
            before,
            r.node_count()
        );
        // Back to root + one compressed edge + leaf.
        assert_eq!(r.node_count(), 2);
        assert_eq!(r.remove(b"foobar"), Some(1));
        assert!(r.is_empty());
        assert_eq!(r.node_count(), 1, "only the root should remain");
        assert!(r.remove(b"foobar").is_none());
    }

    #[test]
    fn remove_an_interior_key_keeps_its_children() {
        let mut r = Rax::new();
        r.insert(b"a", 1);
        r.insert(b"ab", 2);
        r.insert(b"abc", 3);
        assert_eq!(r.remove(b"ab"), Some(2));
        assert_eq!(strs(&r), vec!["a", "abc"]);
        assert_eq!(r.find(b"a"), Some(&1));
        assert_eq!(r.find(b"abc"), Some(&3));
        assert_eq!(r.remove(b"a"), Some(1));
        assert_eq!(strs(&r), vec!["abc"]);
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn many_keys_round_trip() {
        let mut r = Rax::new();
        let mut model: Vec<String> = Vec::new();
        for i in 0..2000u32 {
            let k = format!("key:{i:08}:{}", i % 7);
            r.insert(k.as_bytes(), i);
            model.push(k);
        }
        model.sort();
        assert_eq!(r.len(), 2000);
        assert_eq!(strs(&r), model);
        for (i, k) in (0..2000u32).map(|i| (i, format!("key:{i:08}:{}", i % 7))) {
            assert_eq!(r.find(k.as_bytes()), Some(&i));
        }
        // Remove half and re-check.
        for i in (0..2000u32).step_by(2) {
            let k = format!("key:{i:08}:{}", i % 7);
            assert_eq!(r.remove(k.as_bytes()), Some(i));
        }
        assert_eq!(r.len(), 1000);
        let mut want: Vec<String> = (1..2000u32)
            .step_by(2)
            .map(|i| format!("key:{i:08}:{}", i % 7))
            .collect();
        want.sort();
        assert_eq!(strs(&r), want);
    }

    #[test]
    fn reverse_iteration() {
        let r = build(&["a", "ab", "abc", "b", "ba", "z"]);
        let mut out = Vec::new();
        let mut it = r.iter_rev();
        while let Some((k, _)) = it.next() {
            out.push(String::from_utf8_lossy(k).into_owned());
        }
        assert_eq!(out, vec!["z", "ba", "b", "abc", "ab", "a"]);
    }

    #[test]
    fn seek_ge_and_gt() {
        let r = build(&["aa", "ab", "b", "bb", "cc"]);
        let first = |op, k: &str| {
            let mut it = r.seek(op, k.as_bytes());
            it.next()
                .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        };
        assert_eq!(first(Seek::Ge, ""), Some("aa".into()));
        assert_eq!(first(Seek::Ge, "aa"), Some("aa".into()));
        assert_eq!(first(Seek::Gt, "aa"), Some("ab".into()));
        assert_eq!(first(Seek::Ge, "aaa"), Some("ab".into()));
        assert_eq!(first(Seek::Ge, "ac"), Some("b".into()));
        assert_eq!(first(Seek::Ge, "b"), Some("b".into()));
        assert_eq!(first(Seek::Gt, "b"), Some("bb".into()));
        assert_eq!(first(Seek::Ge, "bc"), Some("cc".into()));
        assert_eq!(first(Seek::Ge, "cc"), Some("cc".into()));
        assert_eq!(first(Seek::Gt, "cc"), None);
        assert_eq!(first(Seek::Ge, "d"), None);
        assert_eq!(first(Seek::Ge, "zzzz"), None);
    }

    #[test]
    fn seek_le_and_lt() {
        let r = build(&["aa", "ab", "b", "bb", "cc"]);
        let first = |op, k: &str| {
            let mut it = r.seek_rev(op, k.as_bytes());
            it.next()
                .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        };
        assert_eq!(first(Seek::Le, "zz"), Some("cc".into()));
        assert_eq!(first(Seek::Le, "cc"), Some("cc".into()));
        assert_eq!(first(Seek::Lt, "cc"), Some("bb".into()));
        assert_eq!(first(Seek::Le, "cb"), Some("bb".into()));
        assert_eq!(first(Seek::Le, "bb"), Some("bb".into()));
        assert_eq!(first(Seek::Le, "ba"), Some("b".into()));
        assert_eq!(first(Seek::Le, "b"), Some("b".into()));
        assert_eq!(first(Seek::Lt, "b"), Some("ab".into()));
        assert_eq!(first(Seek::Le, "aaa"), Some("aa".into()));
        assert_eq!(first(Seek::Le, "aa"), Some("aa".into()));
        assert_eq!(first(Seek::Lt, "aa"), None);
        assert_eq!(first(Seek::Le, "a"), None);
        assert_eq!(first(Seek::Le, ""), None);
    }

    #[test]
    fn seek_then_iterate_is_a_range_scan() {
        let r = build(&["k1", "k2", "k3", "k4", "k5"]);
        // XRANGE k2 k4
        let mut out = Vec::new();
        let mut it = r.seek(Seek::Ge, b"k2");
        while let Some((k, _)) = it.next() {
            if k > b"k4".as_slice() {
                break;
            }
            out.push(String::from_utf8_lossy(k).into_owned());
        }
        assert_eq!(out, vec!["k2", "k3", "k4"]);

        // XREVRANGE k4 k2
        let mut out = Vec::new();
        let mut it = r.seek_rev(Seek::Le, b"k4");
        while let Some((k, _)) = it.next() {
            if k < b"k2".as_slice() {
                break;
            }
            out.push(String::from_utf8_lossy(k).into_owned());
        }
        assert_eq!(out, vec!["k4", "k3", "k2"]);
    }

    /// Regression, found by `prop_rax_seek_matches_btreemap_bounds`.
    ///
    /// At a branching node the target byte can fall *between* two child edges,
    /// so the children below it and the children above it both exist. The
    /// first version of the seek modelled the divergence as one-or-the-other,
    /// which made a backward seek that diverged upward give up and report EOF
    /// instead of descending into the child below.
    #[test]
    fn seek_diverging_between_two_child_edges() {
        // Edges 'a' (0x61) and 0xc6 hang off the root; the target 'b' (0x62)
        // sits between them.
        let mut r: Rax<u32> = Rax::new();
        r.insert(&[0x61], 1);
        r.insert(&[0xc6], 2);

        let mut it = r.seek_rev(Seek::Le, &[0x62]);
        assert_eq!(it.next().map(|(k, _)| k.to_vec()), Some(vec![0x61]));
        let mut it = r.seek_rev(Seek::Lt, &[0x62]);
        assert_eq!(it.next().map(|(k, _)| k.to_vec()), Some(vec![0x61]));
        let mut it = r.seek(Seek::Ge, &[0x62]);
        assert_eq!(it.next().map(|(k, _)| k.to_vec()), Some(vec![0xc6]));
        let mut it = r.seek(Seek::Gt, &[0x62]);
        assert_eq!(it.next().map(|(k, _)| k.to_vec()), Some(vec![0xc6]));

        // Same shape with more edges, diverging in the interior.
        let r = build(&["a", "c", "e", "g"]);
        let le = |k: &str| {
            let mut it = r.seek_rev(Seek::Le, k.as_bytes());
            it.next()
                .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        };
        let ge = |k: &str| {
            let mut it = r.seek(Seek::Ge, k.as_bytes());
            it.next()
                .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        };
        assert_eq!(le("b"), Some("a".into()));
        assert_eq!(le("d"), Some("c".into()));
        assert_eq!(le("f"), Some("e".into()));
        assert_eq!(le("h"), Some("g".into()));
        assert_eq!(le("A"), None);
        assert_eq!(ge("b"), Some("c".into()));
        assert_eq!(ge("d"), Some("e".into()));
        assert_eq!(ge("f"), Some("g".into()));
        assert_eq!(ge("h"), None);
    }

    #[test]
    fn seek_inside_a_compressed_label() {
        // "abcdef" is one compressed edge; seeking to a point inside it must
        // still land correctly.
        let r = build(&["abcdef", "abcxyz", "zz"]);
        let first = |op, k: &str| {
            let mut it = r.seek(op, k.as_bytes());
            it.next()
                .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        };
        assert_eq!(first(Seek::Ge, "abc"), Some("abcdef".into()));
        assert_eq!(first(Seek::Ge, "abcd"), Some("abcdef".into()));
        assert_eq!(first(Seek::Ge, "abcde"), Some("abcdef".into()));
        assert_eq!(first(Seek::Ge, "abcdf"), Some("abcxyz".into()));
        assert_eq!(first(Seek::Ge, "abcy"), Some("zz".into()));
        assert_eq!(first(Seek::Ge, "abd"), Some("zz".into()));

        let last = |op, k: &str| {
            let mut it = r.seek_rev(op, k.as_bytes());
            it.next()
                .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        };
        assert_eq!(last(Seek::Le, "abcdf"), Some("abcdef".into()));
        assert_eq!(last(Seek::Le, "abcd"), None);
        assert_eq!(last(Seek::Le, "abd"), Some("abcxyz".into()));
        assert_eq!(last(Seek::Le, "zy"), Some("abcxyz".into()));
    }

    #[test]
    fn stream_id_shaped_keys() {
        // 128-bit big-endian IDs: lexicographic order == numeric order.
        let mut r = Rax::new();
        let id = |ms: u64, seq: u64| {
            let mut b = [0u8; 16];
            b[..8].copy_from_slice(&ms.to_be_bytes());
            b[8..].copy_from_slice(&seq.to_be_bytes());
            b
        };
        let ids: Vec<[u8; 16]> = (0..500u64)
            .map(|i| id(1700000000000 + i / 5, i % 5))
            .collect();
        for (i, k) in ids.iter().enumerate() {
            r.insert(k, i as u32);
        }
        assert_eq!(r.len(), 500);
        // Iteration order is numeric order.
        let mut it = r.iter();
        let mut prev: Option<Vec<u8>> = None;
        let mut n = 0;
        while let Some((k, _)) = it.next() {
            if let Some(p) = &prev {
                assert!(k > p.as_slice());
            }
            prev = Some(k.to_vec());
            n += 1;
        }
        assert_eq!(n, 500);
        // XRANGE from a mid ID.
        let start = id(1700000000050, 0);
        let mut it = r.seek(Seek::Ge, &start);
        let (k, v) = it.next().expect("found");
        assert_eq!(k, &start[..]);
        assert_eq!(*v, 250);
        // Exclusive start, as XRANGE ( does.
        let mut it = r.seek(Seek::Gt, &start);
        let (k, _) = it.next().expect("found");
        assert_eq!(k, &id(1700000000050, 1)[..]);
    }

    #[test]
    fn arena_slots_are_reused() {
        let mut r = Rax::new();
        for i in 0..500u32 {
            r.insert(format!("k{i:04}").as_bytes(), i);
        }
        let peak = r.nodes.len();
        for i in 0..500u32 {
            r.remove(format!("k{i:04}").as_bytes());
        }
        assert!(r.is_empty());
        assert_eq!(r.node_count(), 1);
        for i in 0..500u32 {
            r.insert(format!("k{i:04}").as_bytes(), i);
        }
        assert_eq!(r.nodes.len(), peak, "free list should have been reused");
    }

    #[test]
    fn clear_and_rebuild() {
        let mut r = build(&["a", "b", "c"]);
        r.clear();
        assert!(r.is_empty());
        assert_eq!(r.node_count(), 1);
        assert!(r.find(b"a").is_none());
        r.insert(b"x", 1);
        assert_eq!(strs(&r), vec!["x"]);
    }

    #[test]
    fn keys_that_are_prefixes_of_each_other() {
        let r = build(&["a", "aa", "aaa", "aaaa"]);
        assert_eq!(strs(&r), vec!["a", "aa", "aaa", "aaaa"]);
        let mut r = r;
        assert_eq!(r.remove(b"aa"), Some(1));
        assert_eq!(strs(&r), vec!["a", "aaa", "aaaa"]);
        assert_eq!(r.remove(b"aaaa"), Some(3));
        assert_eq!(strs(&r), vec!["a", "aaa"]);
        assert_eq!(r.find(b"aaa"), Some(&2));
    }

    #[test]
    fn binary_keys_with_nul_and_high_bytes() {
        let mut r = Rax::new();
        let ks: Vec<Vec<u8>> = vec![
            vec![0],
            vec![0, 0],
            vec![0, 255],
            vec![255],
            vec![255, 0],
            vec![1, 2, 3],
        ];
        for (i, k) in ks.iter().enumerate() {
            r.insert(k, i as u32);
        }
        let mut sorted = ks.clone();
        sorted.sort();
        assert_eq!(keys(&r), sorted);
        for (i, k) in ks.iter().enumerate() {
            assert_eq!(r.find(k), Some(&(i as u32)));
        }
    }
}
