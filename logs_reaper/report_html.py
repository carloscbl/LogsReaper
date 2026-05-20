"""Reporte HTML single-file (offline).

Diseñado para correr sin Streamlit / sin server: un único `.html` con:

* Resumen del run (per-service delta kind, summary counts).
* Top code-errors (con traceback formateado).
* Top policy-violations.
* Sección de baseline (tamaños, paths).

Plotly se carga desde CDN (sólo si hay conexión); si no hay, las gráficas se
omiten pero el resto del informe sigue siendo legible.
"""
from __future__ import annotations

import html
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_CSS = """
:root { --fg:#222; --muted:#666; --safe:#198754; --unsafe:#dc3545; --neutral:#6c757d; }
body { font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
       margin: 24px; color: var(--fg); max-width: 1100px; }
h1, h2, h3 { line-height: 1.25; }
header { border-bottom: 1px solid #ddd; padding-bottom: 12px; margin-bottom: 24px; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 999px;
         color: white; font-size: 12px; font-weight: 600; }
.badge.safe     { background: var(--safe); }
.badge.unsafe   { background: var(--unsafe); }
.badge.no-change { background: var(--neutral); }
.svc { margin: 18px 0; padding: 16px; border: 1px solid #eee; border-radius: 6px; }
.muted { color: var(--muted); font-size: 12px; }
table { width: 100%; border-collapse: collapse; margin-top: 8px; font-size: 13px; }
th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #f0f0f0; vertical-align: top; }
th { background: #fafafa; font-weight: 600; }
pre.tb { background: #1e1e1e; color: #f1f1f1; padding: 12px; border-radius: 6px;
         font-size: 12px; overflow-x: auto; }
.kv { display: grid; grid-template-columns: 160px 1fr; gap: 4px 16px; font-size: 13px; }
.kv .k { color: var(--muted); }
"""


def _esc(text: Any) -> str:
    return html.escape("" if text is None else str(text))


def _counts_row(counts: dict[str, Any]) -> str:
    keys = ["new", "missing", "regressed", "severity_shifted",
            "code_errors", "policy_violations", "pinned_missing"]
    return " · ".join(f"{k}=<b>{_esc(counts.get(k, 0))}</b>" for k in keys)


def _service_block(service: str, info: dict[str, Any]) -> str:
    kind = info.get("delta_kind", "?")
    badge_class = {"safe": "safe", "unsafe": "unsafe", "no-change": "no-change"}.get(kind, "no-change")
    counts = info.get("summary_counts", {})
    parts: list[str] = []
    parts.append(f"<section class='svc'><h2>{_esc(service)} "
                 f"<span class='badge {badge_class}'>{_esc(kind)}</span></h2>")
    parts.append(f"<div class='muted'>run_dir: <code>{_esc(info.get('run_dir',''))}</code></div>")
    parts.append(f"<div style='margin-top:6px'>{_counts_row(counts)}</div>")

    ce = info.get("code_errors_top") or []
    if ce:
        parts.append("<h3>Top code errors</h3>")
        parts.append("<table><thead><tr><th>count</th><th>severity</th><th>exception</th><th>template_id</th></tr></thead><tbody>")
        for e in ce:
            parts.append(
                f"<tr><td>{_esc(e.get('observed_count'))}</td>"
                f"<td>{_esc(e.get('severity_text'))}</td>"
                f"<td>{_esc(e.get('exception_type') or e.get('error_kind'))}</td>"
                f"<td><code>{_esc(str(e.get('template_id'))[:16])}…</code></td></tr>"
            )
        parts.append("</tbody></table>")

    pv = info.get("policy_violations_top") or []
    if pv:
        parts.append("<h3>Top policy violations (banned templates)</h3>")
        parts.append("<table><thead><tr><th>count</th><th>severity</th><th>reason</th><th>template_id</th></tr></thead><tbody>")
        for v in pv:
            parts.append(
                f"<tr><td>{_esc(v.get('observed_count'))}</td>"
                f"<td>{_esc(v.get('severity_text'))}</td>"
                f"<td>{_esc(v.get('reason'))}</td>"
                f"<td><code>{_esc(str(v.get('template_id'))[:16])}…</code></td></tr>"
            )
        parts.append("</tbody></table>")

    parts.append("</section>")
    return "\n".join(parts)


def write_report_html(
    *,
    out_path: Path,
    run_id: str,
    per_service: dict[str, dict[str, Any]],
    collect_stats: dict[str, Any],
    registry_dir: Path,
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()
    safe_count = sum(1 for v in per_service.values() if v["delta_kind"] == "safe")
    unsafe_count = sum(1 for v in per_service.values() if v["delta_kind"] == "unsafe")
    nochg_count = sum(1 for v in per_service.values() if v["delta_kind"] == "no-change")

    cs = collect_stats or {}
    bytes_total = sum((cs.get("bytes_per_service") or {}).values())
    lines_total = sum((cs.get("lines_per_service") or {}).values())
    duration = cs.get("duration_seconds")
    header = f"""
    <header>
      <h1>LogsReaper Report — {_esc(run_id)}</h1>
      <div class='muted'>generated at {_esc(generated_at)}</div>
      <div style='margin-top:8px'>
        <span class='badge safe'>safe: {safe_count}</span>
        <span class='badge unsafe'>unsafe: {unsafe_count}</span>
        <span class='badge no-change'>no-change: {nochg_count}</span>
      </div>
      <div class='kv' style='margin-top:12px'>
        <div class='k'>collected bytes</div>
        <div>{_esc(bytes_total)}</div>
        <div class='k'>collected lines</div>
        <div>{_esc(lines_total)}</div>
        <div class='k'>duration (s)</div>
        <div>{_esc(duration)}</div>
        <div class='k'>registry dir</div>
        <div><code>{_esc(registry_dir)}</code></div>
      </div>
    </header>
    """

    body_blocks = "\n".join(_service_block(svc, info) for svc, info in sorted(per_service.items()))

    raw_data = json.dumps({
        "run_id": run_id,
        "generated_at": generated_at,
        "per_service": per_service,
        "collect": collect_stats,
    }, indent=2, default=str)

    document = f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"/>
<title>LogsReaper Report {_esc(run_id)}</title>
<style>{_CSS}</style>
</head><body>
{header}
{body_blocks}
<details style='margin-top:32px'><summary>Raw JSON</summary>
<pre>{_esc(raw_data)}</pre></details>
</body></html>
"""
    out_path.write_text(document)
    return out_path
