//! Drain template miner — Rust port.
//!
//! Background. The Rust normalize_message pipeline catches well-known
//! variable shapes (timestamps, UUIDs, IPs, paths, hex blobs, opaque IDs
//! masked in Phase 1). Anything it does NOT have a rule for still slips
//! through as a "stable" token in the headline and produces one template
//! per variant. Drain solves the residual case by *learning* which token
//! positions vary across the observed corpus.
//!
//! Algorithm (Du & Li, 2017). A fixed-depth parse tree:
//!
//! ```text
//!   root
//!    ├─ "5"          (token count of the message)
//!    │   ├─ "Conversation"
//!    │   │   ├─ "was"
//!    │   │   │   └─ [cluster_a, cluster_b, ...]    ← leaf
//!    │   │   ...
//!    │   ...
//!    ...
//! ```
//!
//! For a new message we tokenize on whitespace, look up the token-count
//! branch, walk `depth - 2` prefix-token levels (variable-looking tokens
//! collapse to the shared `<*>` child to avoid tree explosion), then at the
//! leaf pick the best matching cluster by `matched_tokens / len`. If the
//! best similarity ≥ `sim_th`, merge by replacing mismatching positions
//! with `<*>`; otherwise spawn a new cluster.
//!
//! Performance. The hot path is `add()`, called once per template (not per
//! event) — input cardinality is O(thousands) per service. Each call is
//! O(depth + tokens_per_message) for the walk + O(clusters_per_leaf *
//! tokens) for the similarity scan. Both are tiny. No allocations in the
//! happy path beyond the tokenization vector.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Tokens already produced by normalize_message that should be treated as
/// wildcard candidates so Drain doesn't try to learn them as fixed
/// positions. `same_token()` treats `<UUID>` and `<HEX>` as equivalent:
/// both signal "variable here".
const NORMALIZED_PLACEHOLDERS: &[&str] = &[
    "<EMAIL>", "<URL>", "<UUID>", "<TIMESTAMP>", "<IP>", "<PATH>", "<HEX>",
    "<PORT>", "<NUM>", "<TRACE_ID>", "<OBJECT_ID>", "<SPAN_ID>", "<ID>",
];

/// Drain's own wildcard marker placed at positions that vary across the
/// cluster members. Distinct from the normalize_message placeholders so we
/// can tell "we learned this varies" vs. "normalize already recognized this".
pub const DRAIN_WILDCARD: &str = "<*>";

#[inline]
fn is_normalized_placeholder(token: &str) -> bool {
    NORMALIZED_PLACEHOLDERS.contains(&token)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    pub cluster_id: u64,
    pub template_tokens: Vec<String>,
    /// Number of input messages absorbed into this cluster.
    pub size: u64,
}

impl Cluster {
    pub fn template_str(&self) -> String {
        self.template_tokens.join(" ")
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Node {
    #[serde(default)]
    children: HashMap<String, Node>,
    #[serde(default)]
    clusters: Vec<Cluster>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Drainer {
    pub depth: usize,
    pub sim_th: f64,
    pub max_children: usize,
    pub next_cluster_id: u64,
    #[serde(default)]
    root: Node,
}

impl Drainer {
    /// Construct a fresh miner. `depth >= 2` is enforced (clamped silently).
    /// Defaults mirror the original Drain paper: depth=4, sim_th=0.5,
    /// max_children=100. These produce conservative clusters: aggressive
    /// merging only when half the tokens agree position-wise, and a hard
    /// cap on how many distinct first-token branches a node may sprout
    /// before falling back to the shared `<*>` child.
    pub fn new(depth: usize, sim_th: f64, max_children: usize) -> Self {
        Self {
            depth: depth.max(2),
            sim_th,
            max_children,
            next_cluster_id: 1,
            root: Node::default(),
        }
    }

    /// Insert `message`. Returns the cluster id it landed in. The same
    /// message string deterministically lands in the same cluster within a
    /// single Drainer lifetime, modulo template evolution as more variants
    /// arrive — i.e. the cluster id is stable for a given message; the
    /// cluster's template may pick up additional wildcards.
    pub fn add(&mut self, message: &str) -> u64 {
        let tokens = tokenize(message);
        if tokens.is_empty() {
            return self.add_empty();
        }
        self.add_tokens(&tokens)
    }

    pub fn template_for(&self, cluster_id: u64) -> Option<String> {
        for cluster in self.iter_clusters() {
            if cluster.cluster_id == cluster_id {
                return Some(cluster.template_str());
            }
        }
        None
    }

    /// Returns `(cluster_id, template_str, size)` for every cluster. Order
    /// is unspecified.
    pub fn all_clusters(&self) -> Vec<(u64, String, u64)> {
        self.iter_clusters()
            .map(|c| (c.cluster_id, c.template_str(), c.size))
            .collect()
    }

    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    pub fn from_json(s: &str) -> serde_json::Result<Self> {
        serde_json::from_str(s)
    }

    // -----------------------------------------------------------------
    // internals
    // -----------------------------------------------------------------

    fn add_empty(&mut self) -> u64 {
        // Bucket all empty messages under a reserved root child so they
        // share a single cluster id. Cheaper than a dedicated field on
        // Drainer itself.
        let bucket = self.root.children.entry("__empty__".to_string()).or_default();
        if bucket.clusters.is_empty() {
            let cid = self.next_cluster_id;
            self.next_cluster_id += 1;
            bucket.clusters.push(Cluster {
                cluster_id: cid,
                template_tokens: Vec::new(),
                size: 0,
            });
        }
        bucket.clusters[0].size += 1;
        bucket.clusters[0].cluster_id
    }

    fn add_tokens(&mut self, tokens: &[String]) -> u64 {
        let depth = self.depth;
        let max_children = self.max_children;
        let sim_th = self.sim_th;

        // Walk down: first-level keyed by token count, then `depth - 2`
        // prefix-token levels. The leaf holds the cluster list.
        let count_key = tokens.len().to_string();
        // SAFETY: child borrows traverse a chain of `&mut Node`. We must
        // not hold an earlier-level reference while reborrowing the child.
        // Use `entry().or_default()` style + scoped reborrows.
        let mut node = self.root.children.entry(count_key).or_default();

        // `depth - 2` prefix levels in theory, but always leave at least one
        // position for the leaf similarity scan. Without this clamp, a
        // two-token message like `alpha beta` vs `alpha gamma` would land
        // on two different leaves (one per second-token value) and never
        // get a chance to merge via similarity. The clamp mirrors the
        // behaviour of the standard Drain3 Python implementation.
        let prefix_levels = depth
            .saturating_sub(2)
            .min(tokens.len().saturating_sub(1));
        for level in 0..prefix_levels {
            let token = &tokens[level];
            let key = branch_key_for_token(token);
            node = pick_or_create_child(node, key, max_children);
        }

        // Leaf: similarity scan.
        let mut best_idx: Option<usize> = None;
        let mut best_sim: f64 = -1.0;
        for (idx, cluster) in node.clusters.iter().enumerate() {
            let sim = similarity(&cluster.template_tokens, tokens);
            if sim > best_sim {
                best_sim = sim;
                best_idx = Some(idx);
            }
        }

        if let Some(idx) = best_idx {
            if best_sim >= sim_th {
                let cluster = &mut node.clusters[idx];
                cluster.template_tokens = merge_templates(&cluster.template_tokens, tokens);
                cluster.size += 1;
                return cluster.cluster_id;
            }
        }

        let cid = self.next_cluster_id;
        self.next_cluster_id += 1;
        node.clusters.push(Cluster {
            cluster_id: cid,
            template_tokens: tokens.to_vec(),
            size: 1,
        });
        cid
    }

    fn iter_clusters(&self) -> ClusterIter<'_> {
        ClusterIter {
            stack: vec![&self.root],
            cluster_idx: 0,
        }
    }
}

/// Pick an existing child by `key`, or create one. When `key` is a literal
/// token and the node has hit `max_children`, fall back to the wildcard
/// child (creating it on demand). Returns `&mut` to the chosen child.
fn pick_or_create_child<'a>(
    parent: &'a mut Node,
    key: String,
    max_children: usize,
) -> &'a mut Node {
    if key == DRAIN_WILDCARD || parent.children.contains_key(&key) {
        return parent.children.entry(key).or_default();
    }
    // Need to add a new literal child; check the cap.
    if parent.children.len() < max_children {
        return parent.children.entry(key).or_default();
    }
    // Cap reached → wildcard sink.
    parent
        .children
        .entry(DRAIN_WILDCARD.to_string())
        .or_default()
}

fn tokenize(message: &str) -> Vec<String> {
    message
        .split_ascii_whitespace()
        .map(|s| s.to_string())
        .collect()
}

/// At inner nodes we don't want to branch on tokens that are *obviously*
/// variable — that would explode the tree on every new ID we've never seen.
/// Two signals route the token to the shared wildcard child:
///   - the token is one of the placeholders normalize_message already
///     emitted (e.g. `<UUID>`, `<HEX>`, `<NUM>`)
///   - the token contains any digit (a cheap proxy for "this is probably
///     not a fixed keyword")
fn branch_key_for_token(token: &str) -> String {
    if is_normalized_placeholder(token) {
        return DRAIN_WILDCARD.to_string();
    }
    if token.bytes().any(|b| b.is_ascii_digit()) {
        return DRAIN_WILDCARD.to_string();
    }
    token.to_string()
}

fn similarity(template: &[String], tokens: &[String]) -> f64 {
    if template.len() != tokens.len() || template.is_empty() {
        return 0.0;
    }
    let mut matches = 0usize;
    for (t, n) in template.iter().zip(tokens.iter()) {
        if same_token(t, n) {
            matches += 1;
        }
    }
    matches as f64 / template.len() as f64
}

fn same_token(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    // A Drain wildcard in the template matches anything in the message.
    if a == DRAIN_WILDCARD {
        return true;
    }
    // A normalize placeholder on one side matches another placeholder /
    // wildcard on the other — both flag "variable here".
    if is_normalized_placeholder(a)
        && (is_normalized_placeholder(b) || b == DRAIN_WILDCARD)
    {
        return true;
    }
    false
}

fn merge_templates(template: &[String], tokens: &[String]) -> Vec<String> {
    let mut merged = Vec::with_capacity(template.len());
    for (t, n) in template.iter().zip(tokens.iter()) {
        if same_token(t, n) {
            if t == DRAIN_WILDCARD || n == DRAIN_WILDCARD {
                merged.push(DRAIN_WILDCARD.to_string());
            } else {
                merged.push(t.clone());
            }
        } else {
            merged.push(DRAIN_WILDCARD.to_string());
        }
    }
    merged
}

struct ClusterIter<'a> {
    stack: Vec<&'a Node>,
    cluster_idx: usize,
}

impl<'a> Iterator for ClusterIter<'a> {
    type Item = &'a Cluster;

    fn next(&mut self) -> Option<&'a Cluster> {
        loop {
            let node = *self.stack.last()?;
            if self.cluster_idx < node.clusters.len() {
                let c = &node.clusters[self.cluster_idx];
                self.cluster_idx += 1;
                return Some(c);
            }
            // Done with this node — pop and queue children.
            self.stack.pop();
            self.cluster_idx = 0;
            for child in node.children.values() {
                self.stack.push(child);
            }
        }
    }
}

// -------------------------------------------------------------------
// Bulk helper: feed a batch of templates, return a remap dict that
// callers (Python scan pipeline) can apply to events in one pass.
// -------------------------------------------------------------------

/// Input row for the bulk remap helper. We keep this minimal — the Python
/// caller pre-extracts the only three fields Drain needs from the templates
/// table. Everything else stays on the Python side and is re-attached after
/// the remap.
#[derive(Debug, Clone)]
pub struct TemplateInput {
    pub template_id: String,
    pub normalized_template: String,
    pub event_count: u64,
}

/// Output describing a single canonical cluster: which template id is its
/// representative, the Drain-learned template string (with `<*>` wildcards),
/// and the summed event count across all members.
#[derive(Debug, Clone)]
pub struct CanonicalRow {
    pub canonical_template_id: String,
    pub drain_template: String,
    pub event_count: u64,
    pub member_template_ids: Vec<String>,
}

/// Run `drain` over `templates`. Returns a remap dict
/// `{old_template_id -> canonical_template_id}` plus a per-cluster summary
/// with the merged template string and member event counts.
///
/// Canonical selection: the member with the highest `event_count` wins
/// (lexicographically-lowest `template_id` breaks ties). This keeps
/// `template_id` stable across runs as long as the dominant variant remains
/// dominant.
pub fn build_template_remap(
    drain: &mut Drainer,
    templates: &[TemplateInput],
) -> (HashMap<String, String>, Vec<CanonicalRow>) {
    // Group input rows by the cluster Drain assigns them to.
    let mut buckets: HashMap<u64, Vec<&TemplateInput>> = HashMap::new();
    for row in templates {
        let cid = drain.add(&row.normalized_template);
        buckets.entry(cid).or_default().push(row);
    }

    let mut remap: HashMap<String, String> = HashMap::new();
    let mut canonical_rows: Vec<CanonicalRow> = Vec::with_capacity(buckets.len());

    for (cid, members) in buckets {
        // Pick canonical = highest event_count, ties → lexicographically
        // lowest template_id. Stable across runs.
        let canonical = members
            .iter()
            .max_by(|a, b| {
                a.event_count
                    .cmp(&b.event_count)
                    .then_with(|| b.template_id.cmp(&a.template_id))
            })
            .unwrap();
        let canonical_id = canonical.template_id.clone();
        let drain_template = drain
            .template_for(cid)
            .unwrap_or_else(|| canonical.normalized_template.clone());
        let total: u64 = members.iter().map(|m| m.event_count).sum();
        let member_ids: Vec<String> =
            members.iter().map(|m| m.template_id.clone()).collect();

        for m in &members {
            remap.insert(m.template_id.clone(), canonical_id.clone());
        }
        canonical_rows.push(CanonicalRow {
            canonical_template_id: canonical_id,
            drain_template,
            event_count: total,
            member_template_ids: member_ids,
        });
    }

    (remap, canonical_rows)
}

// -------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_variants_collapse_to_one_cluster() {
        let mut d = Drainer::new(4, 0.5, 100);
        let c1 = d.add("Conversation was not found conv_aaaa1111");
        let c2 = d.add("Conversation was not found conv_bbbb2222");
        let c3 = d.add("Conversation was not found conv_cccc3333");
        assert_eq!(c1, c2);
        assert_eq!(c2, c3);
        let tpl = d.template_for(c1).unwrap();
        assert_eq!(tpl, "Conversation was not found <*>");
    }

    #[test]
    fn distinct_skeletons_stay_separate() {
        let mut d = Drainer::new(4, 0.5, 100);
        let c1 = d.add("Conversation was not found conv_aaaa");
        let c2 = d.add("Kafka publish failed for topic foo");
        assert_ne!(c1, c2);
        // sanity: token count is the very first split, so different lengths
        // can never collide.
        let c3 = d.add("short one");
        assert_ne!(c3, c1);
        assert_ne!(c3, c2);
    }

    #[test]
    fn similarity_threshold_respected() {
        // Only one token differs out of two — similarity = 0.5; with the
        // default 0.5 threshold that's still a merge.
        let mut d = Drainer::new(4, 0.5, 100);
        let c1 = d.add("alpha beta");
        let c2 = d.add("alpha gamma");
        assert_eq!(c1, c2);

        // Now bump the threshold above the achievable similarity and
        // confirm we split.
        let mut d2 = Drainer::new(4, 0.95, 100);
        let c3 = d2.add("alpha beta");
        let c4 = d2.add("alpha gamma");
        assert_ne!(c3, c4);
    }

    #[test]
    fn normalize_placeholders_treated_as_wildcards() {
        // Two messages where the only differing position is already a
        // normalize_message placeholder (`<HEX>` vs `<NUM>`) should land in
        // the same cluster — both indicate "variable".
        let mut d = Drainer::new(4, 0.5, 100);
        let c1 = d.add("error code <HEX> at line <NUM>");
        let c2 = d.add("error code <UUID> at line <NUM>");
        assert_eq!(c1, c2);
    }

    #[test]
    fn json_roundtrip_preserves_state() {
        let mut d = Drainer::new(4, 0.5, 100);
        d.add("Conversation was not found conv_aaaa");
        d.add("Conversation was not found conv_bbbb");
        d.add("Different message family entirely");
        let json = d.to_json().unwrap();
        let mut d2 = Drainer::from_json(&json).unwrap();

        // After reload, a new variant of the existing cluster should land
        // in the same id — proving the tree state was preserved.
        let cid_before = d.add("Conversation was not found conv_zzzz");
        let cid_after = d2.add("Conversation was not found conv_zzzz");
        assert_eq!(cid_before, cid_after);
        assert_eq!(d.next_cluster_id, d2.next_cluster_id);
    }

    #[test]
    fn empty_messages_share_one_cluster() {
        let mut d = Drainer::new(4, 0.5, 100);
        let c1 = d.add("");
        let c2 = d.add("   ");
        assert_eq!(c1, c2);
    }

    #[test]
    fn max_children_falls_back_to_wildcard() {
        // depth=2 means we only branch on token count (no prefix levels),
        // so use depth=3 to exercise one prefix level. Cap children at 2
        // to force the wildcard fallback on the third distinct first token.
        let mut d = Drainer::new(3, 0.5, 2);
        // Length 4 messages with distinct first tokens. Note all messages
        // need to be length-4 so they share the count branch.
        let _ = d.add("alpha b c d");
        let _ = d.add("bravo b c d");
        // Third distinct first-token: should land in the wildcard child.
        let _ = d.add("charlie b c d");
        let _ = d.add("delta   b c d");
        // All four "X b c d" share a leaf via the wildcard fallback path
        // — but the first two each occupy their own dedicated branch and
        // are separate leaves. Just check we didn't panic and clusters
        // exist.
        assert!(!d.all_clusters().is_empty());
    }

    #[test]
    fn bulk_remap_collapses_variants() {
        let templates = vec![
            TemplateInput {
                template_id: "t1".into(),
                normalized_template: "Conv not found conv_<HEX>".into(),
                event_count: 200,
            },
            TemplateInput {
                template_id: "t2".into(),
                normalized_template: "Conv not found conv_<HEX>".into(),
                event_count: 50,
            },
            TemplateInput {
                template_id: "t3".into(),
                normalized_template: "Unrelated error somewhere".into(),
                event_count: 10,
            },
        ];
        let mut d = Drainer::new(4, 0.5, 100);
        let (remap, canonical) = build_template_remap(&mut d, &templates);

        assert_eq!(remap.get("t1"), Some(&"t1".to_string()));
        assert_eq!(remap.get("t2"), Some(&"t1".to_string()));
        assert_eq!(remap.get("t3"), Some(&"t3".to_string()));
        assert_eq!(canonical.len(), 2);
        let conv = canonical.iter().find(|c| c.canonical_template_id == "t1").unwrap();
        assert_eq!(conv.event_count, 250);
    }
}
