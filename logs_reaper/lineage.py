"""Template lineage: link a "novel" template to its closest predecessor.

Today's normalization (regex over timestamps/UUIDs/IPs/numeric literals)
collapses superficial changes, but a refactor that adds a word, swaps a verb,
or translates a string produces a different ``template_id`` even when humans
see the same incident. This module computes a cheap lexical similarity over a
shingled bag-of-tokens representation and exposes a "find me the closest
baseline template" helper that the diff engine uses to relabel a "new"
template as "evolved from <predecessor>".

The default similarity is Jaccard over 2-grams of normalized tokens — no
external model dependency, sub-millisecond per comparison. Switching to a real
embedding (BGE / E5) is one function swap away when we want it.
"""
from __future__ import annotations

import re
from typing import Any, Iterable


_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def tokenize(text: str) -> list[str]:
    if not text:
        return []
    return [token.lower() for token in _TOKEN_RE.findall(text) if token]


def shingles(tokens: list[str], n: int = 2) -> set[str]:
    """Return n-gram shingles plus the bare tokens themselves.

    Bare tokens make the score robust for very short templates where you can't
    extract enough n-grams to discriminate.
    """
    if not tokens:
        return set()
    result: set[str] = set(tokens)
    if n <= 1:
        return result
    for i in range(len(tokens) - n + 1):
        result.add(" ".join(tokens[i : i + n]))
    return result


def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def template_shingles(normalized_template: str, *, n: int = 2) -> set[str]:
    return shingles(tokenize(normalized_template), n=n)


def find_predecessor(
    *,
    candidate_template_id: str,
    candidate_shingles: set[str],
    baseline_templates: Iterable[dict[str, Any]],
    min_similarity: float = 0.5,
    require_same_severity: bool = True,
    candidate_severity: str | None = None,
) -> dict[str, Any] | None:
    """Return {predecessor_template_id, similarity_score, normalized_template}
    or None if no candidate clears the threshold.
    """
    best: dict[str, Any] | None = None
    best_score = min_similarity
    for base in baseline_templates:
        base_id = str(base.get("template_id") or "")
        if not base_id or base_id == candidate_template_id:
            continue
        if require_same_severity and candidate_severity:
            if str(base.get("severity_text") or "") != candidate_severity:
                continue
        base_shingles = base.get("_shingles")
        if base_shingles is None:
            base_shingles = template_shingles(str(base.get("normalized_template") or ""))
            base["_shingles"] = base_shingles
        score = jaccard(candidate_shingles, base_shingles)
        if score > best_score:
            best_score = score
            best = {
                "predecessor_template_id": base_id,
                "similarity_score": score,
                "normalized_template": base.get("normalized_template"),
            }
    return best


def annotate_diff_with_lineage(
    diff: dict[str, Any],
    baseline_for_cohort: dict[str, dict[str, Any]],
    *,
    min_similarity: float = 0.5,
    require_same_severity: bool = True,
) -> dict[str, Any]:
    """In-place: enrich diff['new_templates'] entries that have a likely
    predecessor in the baseline cohort with `predecessor_template_id` and
    `similarity_score`. The ``kind`` field is also flipped from
    ``new_template`` to ``evolved_template`` for those entries when persisted
    via ``diff_to_table``.
    """
    if not diff.get("new_templates"):
        return diff
    baseline_list = list(baseline_for_cohort.values())
    # Precompute shingles for baseline templates once.
    for base in baseline_list:
        base.setdefault("_shingles", template_shingles(str(base.get("normalized_template") or "")))
    for entry in diff["new_templates"]:
        candidate_shingles = template_shingles(str(entry.get("normalized_template") or ""))
        predecessor = find_predecessor(
            candidate_template_id=str(entry.get("template_id") or ""),
            candidate_shingles=candidate_shingles,
            baseline_templates=baseline_list,
            min_similarity=min_similarity,
            require_same_severity=require_same_severity,
            candidate_severity=str(entry.get("severity_text") or ""),
        )
        if predecessor is not None:
            entry["predecessor_template_id"] = predecessor["predecessor_template_id"]
            entry["similarity_score"] = predecessor["similarity_score"]
            entry["predecessor_normalized_template"] = predecessor["normalized_template"]
            entry["kind"] = "evolved_template"
    # Drop the helper key we attached to baseline rows to keep the structure clean.
    for base in baseline_list:
        base.pop("_shingles", None)
    return diff
