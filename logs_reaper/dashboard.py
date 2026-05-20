"""Streamlit dashboard for LogsReaper.

Launch via ``logs-reaper dashboard`` (or ``streamlit run dashboard.py`` with
``LOGS_REAPER_REGISTRY`` exported). Reads parquet from the registry directory
written by ``logs-reaper index``.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Streamlit launches this file as a top-level script (no package context), so
# relative imports fail. Make `logs_reaper.*` importable both ways: when this
# module is loaded by `streamlit run dashboard.py` and when it is imported as
# `logs_reaper.dashboard`.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq

from logs_reaper.dashboard_data import (
    connectivity_gantt,
    filter_runs,
    heatmap_matrix,
    list_scenarios,
    list_services,
    novelty_curve,
    regression_burndown,
    survival_post_boot,
)
from logs_reaper.diff_engine import load_baseline_for
from logs_reaper.overrides import (
    list_overrides_for,
    load_overrides,
    save_overrides,
    set_override,
)


@st.cache_data(show_spinner=False)
def _load_registry_cached(path: str, mtime: float = 0.0) -> object:
    # mtime se incluye en la clave para invalidar el cache automáticamente
    # cuando el auto-indexer reescribe el parquet.
    return pq.read_table(path)


def _read_parquet_fresh(path: Path):
    """Lee parquet por path con mtime como cache-buster — auto-invalida tras
    cada nuevo write del auto-indexer."""
    try:
        return _load_registry_cached(str(path), path.stat().st_mtime)
    except FileNotFoundError:
        return None


def _mtime_or_zero(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


@st.cache_data(show_spinner=False, max_entries=128)
def _compute_diff_cached(
    run_dir_str: str,
    baseline_path_str: str,
    run_json_mtime: float,
    templates_mtime: float,
    baseline_mtime: float,
) -> dict[str, object]:
    """`diff_engine.compute_diff` keyed on the mtimes of every file it
    reads (run.json + templates.parquet + baseline.parquet).

    Without this cache the Code-Errors tab fired one compute_diff per
    service per fragment rerun — with ~30 services and a 5-second
    refresh cadence, the baseline.parquet was being parsed ~6×/s. That
    parse was the dominant cost of the dashboard CPU spikes the user
    saw. The cache lets identical inputs return instantly; mtimes
    ensure the next materialize evicts only the affected entries.
    """
    from logs_reaper.diff_engine import compute_diff as _compute_diff

    return _compute_diff(
        run_dir=Path(run_dir_str),
        baseline_path=Path(baseline_path_str),
    )


def _size_str(path: Path) -> str:
    if not path.exists():
        return "—"
    size = path.stat().st_size
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size/1024:.1f} KB"
    return f"{size/(1024*1024):.1f} MB"


@st.cache_data(show_spinner=False, max_entries=64)
def _load_event_bodies_map(run_dir_str: str, mtime: float) -> dict[str, str]:
    """Eager dict of {event_id: body} for one service's events.parquet.

    Cached per (run_dir, mtime) so a single read covers every error
    expander in the Code-Errors tab — the previous version did one
    `pq.read_table` + `to_pylist()` + linear `.index()` per event_id,
    which with 20 visible errors × 30 services × the auto-refresh fragment
    rerun was the largest single contributor to the dashboard CPU spikes.
    `mtime` keys invalidate after every materialize so the dict stays
    fresh without manual purges.
    """
    path = Path(run_dir_str) / "events.parquet"
    if not path.exists():
        return {}
    try:
        table = pq.read_table(path, columns=["event_id", "body"])
    except Exception:
        return {}
    ids = table.column("event_id").to_pylist()
    bodies = table.column("body").to_pylist()
    return {eid: body for eid, body in zip(ids, bodies) if eid}


def _resolve_event_body(run_dir_str: str, example_event_id: str) -> str | None:
    """Look up the original multi-line body for an example event.

    Reuses the cached per-service map; degrades to None when the events
    parquet hasn't been written yet (e.g. first auto-index tick on a
    fresh service)."""
    if not example_event_id:
        return None
    path = Path(run_dir_str) / "events.parquet"
    if not path.exists():
        return None
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    return _load_event_bodies_map(run_dir_str, mtime).get(example_event_id)


import re as _re

# Patterns inserted as newline anchors. Each is matched non-greedily and the
# whitespace around it is collapsed so the result lines up cleanly even when the
# normalizer has already squished the original message into one row.
_TRACEBACK_HEADER = _re.compile(r"\s*(Traceback \(most recent call last\):)\s*")
_FILE_FRAME = _re.compile(r"\s+(File \"[^\"]+\", line \S+?, in \S+)\s+")
_EXC_FINAL = _re.compile(r"\s+([A-Z][A-Za-z0-9_.]+Error[^\n]*?:[^\n]*)$")


def _prettify_traceback(text: str | None) -> str:
    """Re-introduce structural newlines into a normalized one-line traceback."""
    if not text:
        return ""
    # The body column may already contain real \n — keep it as-is in that case.
    if "\n" in text:
        return text
    out = text
    out = _TRACEBACK_HEADER.sub(r"\n\1\n  ", out)
    # Walk every `File "...", line N, in func` block, splitting it onto its own
    # line and pulling the following code line into an indented child.
    parts = _re.split(r"(\s*File \"[^\"]+\", line \S+?, in \S+\s*)", out)
    rebuilt: list[str] = []
    for piece in parts:
        if _re.match(r"\s*File \"", piece):
            rebuilt.append("\n  " + piece.strip() + "\n    ")
        else:
            rebuilt.append(piece)
    out = "".join(rebuilt)
    # Clean up runs of whitespace introduced inside source-code lines.
    out = _re.sub(r"[ \t]+", " ", out)
    out = _re.sub(r"\n[ \t]*\n", "\n", out)
    return out.strip()


# ---- Jira integration ------------------------------------------------------
# Everything is opt-in and env-driven — no Atlassian URLs hardcoded. The
# dashboard hides the Jira button when JIRA_BASE_URL is empty.
#
# Atlassian Cloud's legacy `secure/CreateIssueDetails!init.jspa?pid=KEY` deep
# link requires the *numeric* project pid (not the project key) and is
# brittle — the modal opens with "no valid project selected" if anything is
# off. The reliable, auth-free flow is:
#   1. Land the user on the parent issue (browse/<PARENT_KEY>).
#   2. Jira's own UI exposes a "Create child" button there.
#   3. The user pastes the markdown summary we've copied to the clipboard.
# That's two clicks + one paste, works on every Cloud workspace, no token.
JIRA_BASE_URL = os.environ.get("LOGS_REAPER_JIRA_BASE", "").rstrip("/")
JIRA_PROJECT_KEY = os.environ.get("LOGS_REAPER_JIRA_PROJECT_KEY", "")
JIRA_PARENT_KEY = os.environ.get("LOGS_REAPER_JIRA_PARENT_KEY", "")
JIRA_ISSUE_TYPE = os.environ.get("LOGS_REAPER_JIRA_ISSUE_TYPE", "Bug")


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def _short_description_from_template(normalized: str | None) -> str:
    """Pick a one-line teaser from the normalized template body to use in
    the Jira issue title. We grab the first non-empty line and squash
    whitespace so the title stays readable."""
    if not normalized:
        return ""
    for raw_line in normalized.splitlines():
        cleaned = " ".join(raw_line.split()).strip()
        if cleaned:
            return cleaned
    return ""


def _build_error_summary_markdown(
    *,
    entry: dict,
    service: str,
    run_id: str,
    example_body: str | None,
) -> tuple[str, str]:
    """Build (jira_summary_line, markdown_description) for a code-error entry.

    The summary is the Jira issue title: `[service] exception — short msg`,
    capped well below the 255-char Jira limit so it stays scannable in the
    backlog. The markdown description embeds every dashboard-visible
    field (template_id, run_id, counts, baseline, classification, the
    deduplicated template, the raw example) plus a Parent line when a
    parent key is configured — that's the contract the user follows when
    they "Create child" from the parent issue.
    """
    exc = entry.get("exception_type") or entry.get("error_kind") or "exception"
    severity = entry.get("severity_text") or "ERROR"
    template_id = str(entry.get("template_id") or "")
    observed = entry.get("observed_count") or 0
    short_tpl = template_id[:12]
    normalized = _prettify_traceback(entry.get("normalized_template"))
    teaser = _short_description_from_template(normalized)

    title = f"[{service}] {exc}"
    if teaser:
        title = f"{title} — {teaser}"
    summary_line = _truncate(title, 200)

    classification = entry.get("classification") or "—"
    classification_reason = entry.get("classification_reason") or "—"
    is_new = "NEW (regression)" if entry.get("is_new") else "REGRESSED"

    lines: list[str] = []
    if JIRA_PARENT_KEY:
        lines.append(f"**Parent:** {JIRA_PARENT_KEY}  ")
    lines += [
        f"**Service:** `{service}`  ",
        f"**Severity:** `{severity}`  ",
        f"**Status:** `{is_new}`  ",
        f"**Exception:** `{exc}`  ",
        "",
        f"**Template ID:** `{template_id}`  ",
        f"**Run ID:** `{run_id}`  ",
        f"**Events observed (this run):** {observed}  ",
        f"**Baseline mean:** {entry.get('baseline_mean') if entry.get('baseline_mean') is not None else '—'}  ",
        f"**First seen:** {entry.get('first_seen_at') or '—'}  ",
        f"**Last seen:** {entry.get('last_seen_at') or '—'}  ",
        "",
        f"**Classification:** {classification} — {classification_reason}",
        "",
        "### Normalized template (deduplicated form)",
        "```",
        normalized or "(empty)",
        "```",
    ]
    if example_body:
        lines += [
            "",
            "### Example raw event",
            "```",
            example_body.strip(),
            "```",
        ]
    lines += [
        "",
        "---",
        f"_Generated by LogsReaper dashboard · service `{service}` · template `{short_tpl}`_",
    ]
    return summary_line, "\n".join(lines)


def _build_jira_button() -> tuple[str, str] | None:
    """Return (button_label, url) when Jira is configured, else None.

    Strategy: open the parent issue page. Jira's own UI shows a "Create
    child" affordance there, which routes to a modal where the user
    pastes our pre-built summary + description. No deep-link guesswork,
    no numeric pid, no token. Falls back to the workspace home when no
    parent is configured."""
    if not JIRA_BASE_URL:
        return None
    if JIRA_PARENT_KEY:
        return (
            f"🪲 Create child of {JIRA_PARENT_KEY}",
            f"{JIRA_BASE_URL}/browse/{JIRA_PARENT_KEY}",
        )
    if JIRA_PROJECT_KEY:
        return (
            f"🪲 Open Jira project {JIRA_PROJECT_KEY}",
            f"{JIRA_BASE_URL}/jira/software/c/projects/{JIRA_PROJECT_KEY}/issues",
        )
    return ("🪲 Open Jira", JIRA_BASE_URL)


def _registry_dir() -> Path:
    raw = os.environ.get("LOGS_REAPER_REGISTRY")
    return Path(raw) if raw else Path(__file__).resolve().parents[1] / "runs"


def _snapshot_search_paths(registry_dir: Path) -> list[Path]:
    """Sitios donde buscar stats_snapshot.json — env var primero, después
    rutas conocidas relativas al árbol del container."""
    paths: list[Path] = []
    env_path = os.environ.get("LOGS_REAPER_SNAPSHOT")
    if env_path:
        paths.append(Path(env_path))
    paths.extend([
        Path("/work/out/logs/live/stats_snapshot.json"),
        registry_dir.parent / "logs" / "live" / "stats_snapshot.json",
        registry_dir / "stats_snapshot.json",
        Path(__file__).resolve().parents[1] / "out_ci" / "logs" / "live" / "stats_snapshot.json",
    ])
    # Dedup keeping order
    seen: set[str] = set()
    unique: list[Path] = []
    for p in paths:
        key = str(p)
        if key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def _load_snapshot(registry_dir: Path) -> tuple[dict | None, Path | None]:
    import json as _json
    for p in _snapshot_search_paths(registry_dir):
        try:
            if p.exists():
                return _json.loads(p.read_text()), p
        except (OSError, _json.JSONDecodeError):
            continue
    return None, None


def _human_bytes(n: float) -> str:
    n = float(n or 0)
    if n < 1024:
        return f"{n:.0f} B"
    if n < 1024 * 1024:
        return f"{n/1024:.1f} KB"
    return f"{n/(1024*1024):.2f} MB"


def _load_auto_index_history(registry_dir: Path) -> dict | None:
    import json as _json
    path = registry_dir / "auto_index_history.json"
    if not path.exists():
        return None
    try:
        return _json.loads(path.read_text())
    except _json.JSONDecodeError:
        return None


def _render_live_ingest(registry_dir: Path) -> None:
    """Tab principal: throughput realtime + agregados del último auto-index."""
    import json as _json
    import time as _time

    st.subheader("Live ingest — runtime stats")
    st.caption(
        "Throughput de `docker logs -f` (StatsTracker, 1-2 Hz) + agregados del último ciclo "
        "del auto-indexer (scan+index cada 5s). Refresca cada N segundos."
    )

    snapshot, snapshot_path = _load_snapshot(registry_dir)
    history = _load_auto_index_history(registry_dir)

    if snapshot is None and history is None:
        st.info(
            "Aún no hay snapshot ni historial de auto-index. Lanza el container "
            "con `logsreaper up` o `logs-reaper live`."
        )
        st.caption("Rutas buscadas:")
        for p in _snapshot_search_paths(registry_dir):
            st.code(str(p), language="text")
        return

    snap_age = _time.time() - snapshot_path.stat().st_mtime if snapshot_path else None
    config = (snapshot or {}).get("config") or {}
    state = (config.get("ingestion_state") or "running").upper()
    badge_color = "#198754" if state == "RUNNING" else "#6c757d"
    totals = (snapshot or {}).get("totals") or {}

    # Métricas top-line: stream + cycle counters
    cols = st.columns([2, 2, 2, 2, 2, 2])
    cols[0].markdown(
        f"<span style='background:{badge_color};color:white;padding:4px 12px;"
        f"border-radius:999px;font-weight:600;'>{state}</span>",
        unsafe_allow_html=True,
    )
    cols[1].metric("Services", totals.get("services", 0))
    cols[2].metric("Total bytes", _human_bytes(totals.get("bytes_total", 0)))
    cols[3].metric("Total lines", totals.get("lines_total", 0))
    cols[4].metric("Uptime", f"{(snapshot or {}).get('uptime_seconds', 0):.0f} s")

    if history is not None:
        runs_completed = history.get("runs_completed", 0)
        interval = history.get("interval_seconds", 0)
        cols[5].metric("Index cycles", f"{runs_completed} (every {interval:.0f}s)")
    else:
        cols[5].metric("Index cycles", "—")

    if snap_age is not None:
        if snap_age > 10:
            st.warning(f"Snapshot stale (last update {snap_age:.0f}s ago) — collector puede estar caído.")
        else:
            st.caption(f"Stream snapshot age: {snap_age:.1f}s · path: `{snapshot_path}`")

    # Fusión: cada servicio se enriquece con el último entry del auto-index.
    services_stream = (snapshot or {}).get("services") or {}
    current_by_svc: dict[str, dict] = {}
    if history is not None:
        current_by_svc = history.get("current") or {}

    universe = set(services_stream.keys()) | set(current_by_svc.keys())
    rows: list[dict] = []
    for svc in sorted(universe):
        stream = services_stream.get(svc) or {}
        rates = stream.get("rates") or {}
        r5 = rates.get("5s") or {}
        r30 = rates.get("30s") or {}
        idle = stream.get("idle_seconds")
        cycle = (history.get("services", {}).get(svc) or [{}])[-1] if history else {}
        if not cycle and current_by_svc.get(svc):
            cycle = current_by_svc[svc]
        # Mantengo los counts como int para que el Styler los pueda evaluar;
        # los formatos los aplica el dataframe con column_config.
        def _i(v):
            try:
                return int(v)
            except (TypeError, ValueError):
                return None
        rows.append({
            "service": svc,
            "stream B": _human_bytes(stream.get("bytes_total", 0)),
            "stream L": int(stream.get("lines_total", 0) or 0),
            "5s B/s": float(r5.get("bytes_per_sec", 0) or 0),
            "5s L/s": float(r5.get("lines_per_sec", 0) or 0),
            "30s B/s": float(r30.get("bytes_per_sec", 0) or 0),
            "scan events": _i(cycle.get("events")),
            "scan templates": _i(cycle.get("templates")),
            "scan errors": _i(cycle.get("errors")),
            "Δ templates": _i(cycle.get("delta_templates")),
            "Δ errors": _i(cycle.get("delta_errors")),
            "Δ code-err": _i(cycle.get("delta_code_errors")),
            "idle (s)": float(idle) if idle is not None else None,
        })
    if rows:
        import pandas as pd
        st.markdown("##### Per-service stream + last scan cycle")
        df = pd.DataFrame(rows)

        def _row_style(r):
            err_keys = ("scan errors", "Δ errors", "Δ code-err")
            bad = any((r.get(k) or 0) > 0 for k in err_keys)
            return [
                "background-color: #ffe5e5; color:#7a0000" if bad else ""
                for _ in r
            ]

        st.dataframe(
            df.style.apply(_row_style, axis=1).format(
                {
                    "5s B/s": "{:.0f}",
                    "5s L/s": "{:.1f}",
                    "30s B/s": "{:.0f}",
                    "idle (s)": "{:.1f}",
                },
                na_rep="—",
            ),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No services registered yet.")

    # Aviso si el auto-index aún no ha corrido.
    if history is None:
        st.info("`auto_index_history.json` ausente — primer ciclo aún no ha terminado.")
    else:
        st.caption(
            f"Index cycles: {history.get('runs_completed', 0)} · "
            f"min_green_runs={history.get('min_green_runs','?')} · "
            f"last_error: {history.get('last_error') or 'none'}"
        )

    # Configuración expandible.
    with st.expander("Configuration"):
        st.json(config)

    with st.expander("Raw snapshot (stream + cycle history)"):
        st.code(_json.dumps({"snapshot": snapshot, "history": history}, indent=2, default=str)[:8000], language="json")


def _no_registry_notice(registry_dir: Path) -> None:
    """Mensaje uniforme para tabs que necesitan registry pero éste aún no existe."""
    st.info(
        "No hay `registry.parquet` todavía. La captura en curso se ve en "
        "**Live Ingest**. Para poblar esta tab, indexa cuando tengas al menos un "
        "scan completo:"
    )
    st.code(
        f"logs-reaper index --root {registry_dir.parent / 'out'} --out {registry_dir} "
        "--baselines-dir ./baselines",
        language="bash",
    )


def _no_runs_notice() -> None:
    st.info("Registry presente pero sin runs en este cohort. Lanza `logs-reaper scan` para generar uno.")


def _inline_service_picker(
    services_list: list[str],
    registry_table,
    baseline_table,
    key_suffix: str,
    *,
    with_scenario: bool = True,
):
    """Selector inline service+scenario para tabs que necesitan cohorte.

    Devuelve (service, scenario, runs, baseline_for_cohort). Si no hay servicios
    devuelve (None, None, [], {}).
    """
    if not services_list or registry_table is None:
        return None, None, [], {}
    cols = st.columns([2, 2] if with_scenario else [2])
    service = cols[0].selectbox("Service", services_list, index=0, key=f"{key_suffix}_svc")
    scenario = None
    if with_scenario:
        scenarios = list_scenarios(registry_table, service)
        scenario = cols[1].selectbox(
            "Scenario", scenarios or [""], index=0, key=f"{key_suffix}_scn",
        )
    runs = filter_runs(registry_table, service, scenario)
    bfc = (
        load_baseline_for(baseline_table, service, scenario or "")
        if baseline_table is not None else {}
    )
    return service, scenario, runs, bfc


def main() -> None:
    import time as _time

    st.set_page_config(page_title="LogsReaper Dashboard", layout="wide")
    registry_dir = _registry_dir()
    registry_path = registry_dir / "registry.parquet"
    baseline_path = registry_dir / "baseline.parquet"

    # ── Refresh model ───────────────────────────────────────────────────
    # Every rerun of the content fragment re-emits all 10 tabs (st.tabs
    # is NOT lazy: plotly figs, pandas-styled dataframes, 30 svc expanders
    # all reconstruct each time), which is ~3-4 s of CPU per rerun for a
    # 30-service cluster. To keep that cost predictable:
    #   * On first load the script runs once and renders fresh data.
    #   * After that, the only auto-rerun is a safety fallback every
    #     AUTO_REFRESH_INTERVAL_SECS (default 2 min). Anything more
    #     frequent burns CPU without enough new data to justify it.
    #   * The primary refresh path is the "🔄 Refresh now" button — one
    #     click ⇒ one rerun. Users can poke it when they care, leave it
    #     alone when reading tracebacks.
    AUTO_REFRESH_INTERVAL_SECS = 120
    LAST_REFRESH_KEY = "_dashboard_last_refresh_ts"
    if LAST_REFRESH_KEY not in st.session_state:
        st.session_state[LAST_REFRESH_KEY] = _time.time()

    st.sidebar.title("LogsReaper")

    last_refresh_ts = st.session_state[LAST_REFRESH_KEY]
    elapsed = int(_time.time() - last_refresh_ts)
    next_auto = max(AUTO_REFRESH_INTERVAL_SECS - elapsed, 0)

    if st.sidebar.button(
        "🔄  Refresh now", type="primary", use_container_width=True,
        help="Re-read every parquet + rebuild charts. ~3-4 s of CPU.",
    ):
        st.session_state[LAST_REFRESH_KEY] = _time.time()
        # Drop the data caches so the rerun actually picks up new parquets
        # rather than serving stale tables from st.cache_data.
        st.cache_data.clear()
        st.rerun()

    if elapsed < 60:
        last_label = f"{elapsed}s ago"
    elif elapsed < 3600:
        last_label = f"{elapsed // 60}m {elapsed % 60}s ago"
    else:
        last_label = f"{elapsed // 3600}h {(elapsed % 3600) // 60}m ago"
    st.sidebar.markdown(
        f"<div style='font-size:13px;line-height:1.5'>"
        f"<b>Last refresh:</b> {last_label}<br>"
        f"<b>Next auto:</b> in {next_auto}s"
        f"</div>",
        unsafe_allow_html=True,
    )
    st.sidebar.caption(
        "Auto-refresh runs once on load and then every "
        f"{AUTO_REFRESH_INTERVAL_SECS // 60} minute(s). "
        "Click the button for an on-demand refresh."
    )
    st.sidebar.divider()

    def _content() -> None:

        has_registry = registry_path.exists()
        registry_table = None
        baseline_table = None
        services: list[str] = []
        # Snapshot mínimo para que todos los tabs no dependan del sidebar.
        latest_by_svc: dict[str, dict] = {}
        all_rows: list[dict] = []

        if has_registry:
            registry_table = _read_parquet_fresh(registry_path)
            baseline_table = _read_parquet_fresh(baseline_path) if baseline_path.exists() else None
            has_registry = registry_table is not None
            if registry_table is not None:
                services = list_services(registry_table)
                all_rows = registry_table.to_pylist()
                for row in all_rows:
                    svc = row.get("service_name")
                    if not svc:
                        continue
                    key = (row.get("created_at") or "", str(row.get("run_id") or ""))
                    prev = latest_by_svc.get(svc)
                    if prev is None or key > (prev.get("created_at") or "", str(prev.get("run_id") or "")):
                        latest_by_svc[svc] = row

        # Sidebar widgets cannot live inside an st.fragment-wrapped function.
        # Publish the counters via session_state and let main() render them
        # outside the fragment.
        st.session_state["_sidebar_services_indexed"] = len(services)
        st.session_state["_sidebar_total_runs"] = (
            registry_table.num_rows if registry_table is not None else 0
        )
        st.session_state["_sidebar_has_registry"] = bool(has_registry)

        (
            tab_live,
            tab_errors,
            tab_connectivity,
            tab_baseline,
            tab_files,
            tab_heatmap,
            tab_novelty,
            tab_survival,
            tab_burndown,
            tab_drill,
        ) = st.tabs(
            [
                "Live Ingest",
                "Code Errors",
                "Connectivity",
                "Baseline Editor",
                "Files & Registry",
                "Heatmap",
                "Novelty",
                "Survival",
                "Burn-down",
                "Templates",
            ]
        )

        with tab_live:
            _render_live_ingest(registry_dir)

        with tab_errors:
            st.subheader("Code errors — summary across all services")
            st.caption(
                "Toma el último run por servicio del registry y calcula diff vs baseline. "
                "Independiente del selector del sidebar."
            )
            if not has_registry:
                _no_registry_notice(registry_dir)
            else:
                # Último run por servicio. created_at puede no estar setteado al
                # inicio; usamos run_id como fallback de orden.
                latest_by_svc: dict[str, dict] = {}
                for row in registry_table.to_pylist():
                    svc = row.get("service_name")
                    if not svc:
                        continue
                    key = (row.get("created_at") or "", str(row.get("run_id") or ""))
                    prev = latest_by_svc.get(svc)
                    if prev is None:
                        latest_by_svc[svc] = row
                        continue
                    prev_key = (prev.get("created_at") or "", str(prev.get("run_id") or ""))
                    if key > prev_key:
                        latest_by_svc[svc] = row

                # Compute diff por servicio. Hot path: cacheado por (run_dir,
                # baseline) con mtimes de los 3 parquets como cache-key — sin
                # esto, cada fragment rerun reparseaba baseline.parquet entero
                # ~30 veces (una por servicio) y era el spike dominante.
                baseline_mtime = _mtime_or_zero(baseline_path)
                summaries: list[dict] = []
                for svc, run in sorted(latest_by_svc.items()):
                    run_dir = Path(run.get("run_dir") or "")
                    if not run_dir.exists():
                        continue
                    try:
                        diff = _compute_diff_cached(
                            str(run_dir),
                            str(baseline_path),
                            _mtime_or_zero(run_dir / "run.json"),
                            _mtime_or_zero(run_dir / "templates.parquet"),
                            baseline_mtime,
                        )
                    except Exception as exc:
                        summaries.append({
                            "service": svc,
                            "run_id": run.get("run_id"),
                            "_error": str(exc),
                        })
                        continue
                    counts = diff.get("summary_counts") or {}
                    summaries.append({
                        "service": svc,
                        "run_id": run.get("run_id"),
                        "code_err_templates": int(counts.get("code_errors", 0)),
                        "code_err_events": int(counts.get("code_error_events", 0)),
                        "new_templates": int(counts.get("new", 0)),
                        "regressed": int(counts.get("regressed", 0)),
                        "policy_violations": int(counts.get("policy_violations", 0)),
                        "_diff": diff,
                        "_run_dir": run_dir,
                    })

                # Totales top-line.
                total_code = sum(s.get("code_err_templates", 0) for s in summaries)
                total_events = sum(s.get("code_err_events", 0) for s in summaries)
                total_new = sum(s.get("new_templates", 0) for s in summaries)
                total_reg = sum(s.get("regressed", 0) for s in summaries)
                total_pol = sum(s.get("policy_violations", 0) for s in summaries)
                top_cols = st.columns(6)
                top_cols[0].metric("Services scanned", len(summaries))
                top_cols[1].metric("Code-err templates", total_code)
                top_cols[2].metric("Code-err events", total_events)
                top_cols[3].metric("New templates", total_new)
                top_cols[4].metric("Regressions", total_reg)
                top_cols[5].metric("Policy violations", total_pol)

                # Filtros inline.
                filter_cols = st.columns([3, 2, 2])
                svc_choices = [s["service"] for s in summaries]
                picked = filter_cols[0].multiselect(
                    "Filter services (empty = all)", svc_choices, key="errors_svc_filter",
                )
                only_with_errors = filter_cols[1].checkbox(
                    "Only with errors", value=True, key="errors_only_bad",
                )
                text_filter = filter_cols[2].text_input(
                    "Search exception/template", value="", key="errors_text_filter",
                )
                text_filter_lc = text_filter.lower().strip()

                def _has_errors(s: dict) -> bool:
                    return any(s.get(k, 0) > 0 for k in (
                        "code_err_templates", "policy_violations", "regressed",
                    ))

                view = [
                    s for s in summaries
                    if (not picked or s["service"] in picked)
                    and (not only_with_errors or _has_errors(s) or "_error" in s)
                ]

                # Tabla agregada con coloring rojo en filas problemáticas.
                if view:
                    import pandas as pd  # streamlit lo tiene como dep

                    table_rows = [{
                        "service": s["service"],
                        "code_err_templates": s.get("code_err_templates", 0),
                        "code_err_events": s.get("code_err_events", 0),
                        "new_templates": s.get("new_templates", 0),
                        "regressed": s.get("regressed", 0),
                        "policy_violations": s.get("policy_violations", 0),
                        "run_id": s.get("run_id"),
                        "error": s.get("_error", ""),
                    } for s in view]
                    df = pd.DataFrame(table_rows)
                    df = df.sort_values(
                        by=["code_err_templates", "policy_violations", "regressed", "service"],
                        ascending=[False, False, False, True],
                    )

                    def _row_style(row):
                        bad = (
                            (row.get("code_err_templates") or 0) > 0
                            or (row.get("policy_violations") or 0) > 0
                            or (row.get("regressed") or 0) > 0
                            or bool(row.get("error"))
                        )
                        return ["background-color: #ffe5e5; color:#7a0000" if bad else "" for _ in row]

                    st.dataframe(
                        df.style.apply(_row_style, axis=1),
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.success("No services with errors in the latest runs.")

                # Detalle expandible (sólo para servicios con errores/policy).
                detail_view = [s for s in view if _has_errors(s) and "_diff" in s]
                if detail_view:
                    st.markdown("---")
                    st.markdown("##### Detail per service")
                    for s in detail_view:
                        diff = s["_diff"]
                        run_dir = s["_run_dir"]
                        policy_violations = diff.get("policy_violations") or []
                        code_errors = diff.get("code_errors") or []
                        if text_filter_lc:
                            code_errors = [
                                e for e in code_errors
                                if text_filter_lc in str(e.get("exception_type") or "").lower()
                                or text_filter_lc in str(e.get("error_kind") or "").lower()
                                or text_filter_lc in str(e.get("normalized_template") or "").lower()
                                or text_filter_lc in str(e.get("template_id") or "").lower()
                            ]
                        # Label estable: sólo el nombre del servicio. Los counts
                        # cambian entre refreshes (event_count crece) y si forman
                        # parte del label, Streamlit los toma como identidad del
                        # expander y lo cierra al refrescar. Movemos los counts al
                        # body via st.caption justo dentro del expander.
                        headline = f"**{s['service']}**"
                        counts_caption = (
                            f"{s.get('code_err_templates',0)} code-err templates · "
                            f"{s.get('code_err_events',0)} events · "
                            f"{s.get('policy_violations',0)} policy · "
                            f"{s.get('regressed',0)} regressed"
                        )
                        with st.expander(headline, expanded=False):
                            st.caption(counts_caption)
                            if policy_violations:
                                st.markdown("##### Policy violations")
                                for entry in policy_violations[:10]:
                                    # Identity estable = template_id. Los counts
                                    # cambian entre ticks; mantenerlos fuera del
                                    # label para que el expander no se cierre.
                                    label = (
                                        f"`{entry.get('severity_text','')}` `BANNED` — "
                                        f"`{entry.get('template_id','')[:16]}`"
                                    )
                                    with st.expander(label, expanded=False):
                                        st.caption(f"observed_count: {entry.get('observed_count',0)}")
                                        st.markdown(f"**reason:** {entry.get('reason') or '—'}")
                                        st.code(entry.get("normalized_template") or "", language="python")
                            if not code_errors:
                                st.info("No code-classified errors after filter.")
                                continue
                            for entry in code_errors[:20]:
                                label = "NEW" if entry.get("is_new") else "REG"
                                # Label estable: severity + NEW/REG + exception
                                # tipo + template_id (los counts cambian; van al
                                # body via caption).
                                headline_e = (
                                    f"`{entry.get('severity_text','')}` "
                                    f"`{label}` — `{entry.get('exception_type') or entry.get('error_kind') or 'exception'}` "
                                    f"· `{str(entry.get('template_id') or '')[:12]}`"
                                )
                                with st.expander(headline_e, expanded=False):
                                    st.caption(f"observed_count: {entry['observed_count']}")
                                    body = _resolve_event_body(str(run_dir), entry.get("example_event_id") or "")
                                    if body:
                                        st.markdown("**Example event (raw body):**")
                                        st.code(body, language="python")
                                    normalized_pretty = _prettify_traceback(entry.get("normalized_template"))
                                    st.markdown("**Normalized template (deduplicated form):**")
                                    st.code(normalized_pretty, language="python")
                                    meta_cols = st.columns(4)
                                    meta_cols[0].markdown(f"**template_id**\n\n`{entry['template_id'][:16]}…`")
                                    meta_cols[1].markdown(
                                        f"**baseline_mean**\n\n{entry.get('baseline_mean') if entry.get('baseline_mean') is not None else '—'}"
                                    )
                                    meta_cols[2].markdown(f"**first_seen**\n\n{entry.get('first_seen_at') or '—'}")
                                    meta_cols[3].markdown(f"**last_seen**\n\n{entry.get('last_seen_at') or '—'}")

                                    # ---------------- Jira / clipboard ----------------
                                    # Flow per error (auth-free):
                                    #   1. Reveal "📋 Copy summary" → st.code's
                                    #      native copy icon puts the markdown on
                                    #      the clipboard.
                                    #   2. "🪲 Create child of <PARENT_KEY>" opens
                                    #      browse/<PARENT_KEY>; the user clicks
                                    #      Jira's own Create-child button and
                                    #      pastes. Two clicks + one paste.
                                    # When Jira env vars aren't set, the Jira
                                    # button is hidden and only Copy stays.
                                    jira_summary, jira_md = _build_error_summary_markdown(
                                        entry=entry,
                                        service=s["service"],
                                        run_id=str(s.get("_run_id") or s.get("run_id") or ""),
                                        example_body=body,
                                    )
                                    jira_button = _build_jira_button()
                                    action_cols = st.columns([1, 1, 4])
                                    if jira_button is not None:
                                        action_cols[0].link_button(
                                            jira_button[0],
                                            jira_button[1],
                                            help=(
                                                "Opens the parent issue. Use Jira's "
                                                "'Create child' button and paste the "
                                                "summary you copy with the other button."
                                            ),
                                        )
                                    show_copy = action_cols[1].toggle(
                                        "📋 Copy summary",
                                        key=f"copy_{s['service']}_{entry['template_id']}",
                                        help="Reveals the markdown block; use the copy icon in the top-right of the code block.",
                                    )
                                    if show_copy:
                                        st.markdown("**Copy-paste-ready summary** (click the icon in the top-right of the block):")
                                        st.code(f"{jira_summary}\n\n{jira_md}", language="markdown")

        with tab_connectivity:
            st.subheader("Connectivity — incidents across all services")
            st.caption(
                "Incidentes de dependencias (kafka/mongo/elasticsearch) tomados del último run "
                "por servicio. Una fila en rojo significa que ese servicio tiene al menos un "
                "incidente de conectividad en su último ciclo."
            )
            if not has_registry:
                _no_registry_notice(registry_dir)
            elif not latest_by_svc:
                st.info("Registry presente pero sin runs todavía.")
            else:
                from logs_reaper.io import read_json as _read_json

                # Por servicio: lista plana de incidentes desde el último run.
                per_svc_incidents: dict[str, list[dict]] = {}
                per_svc_meta: dict[str, dict] = {}
                for svc, run in latest_by_svc.items():
                    run_dir = Path(run.get("run_dir") or "")
                    meta_path = run_dir / "run.json"
                    if not meta_path.exists():
                        continue
                    try:
                        meta = _read_json(meta_path)
                    except Exception:
                        continue
                    timeline = meta.get("connectivity_timeline") or {}
                    flat: list[dict] = []
                    for dep, payload in timeline.items():
                        if not isinstance(payload, dict):
                            continue
                        for incident in payload.get("incidents") or []:
                            flat.append({
                                "service": svc,
                                "run_id": run.get("run_id"),
                                "dependency": dep,
                                "down_at": incident.get("down_at"),
                                "up_at": incident.get("up_at"),
                                "duration_seconds": incident.get("duration_seconds"),
                            })
                    per_svc_incidents[svc] = flat
                    per_svc_meta[svc] = meta

                # Resumen por servicio.
                summary_rows: list[dict] = []
                for svc, run in sorted(latest_by_svc.items()):
                    incidents = per_svc_incidents.get(svc, [])
                    deps = sorted({i["dependency"] for i in incidents})
                    total_down = sum(
                        float(i.get("duration_seconds") or 0) for i in incidents
                    )
                    summary_rows.append({
                        "service": svc,
                        "incidents": len(incidents),
                        "deps_affected": ", ".join(deps) if deps else "—",
                        "total_downtime_s": round(total_down, 1) if total_down else 0.0,
                        "run_id": run.get("run_id"),
                    })

                total_incidents = sum(r["incidents"] for r in summary_rows)
                total_downtime = sum(r["total_downtime_s"] for r in summary_rows)
                with_inc = sum(1 for r in summary_rows if r["incidents"] > 0)
                top_cols = st.columns(4)
                top_cols[0].metric("Services scanned", len(summary_rows))
                top_cols[1].metric("Services with incidents", with_inc)
                top_cols[2].metric("Total incidents", total_incidents)
                top_cols[3].metric("Total downtime (s)", round(total_downtime, 1))

                # Filtros inline.
                filter_cols = st.columns([3, 2, 2])
                svc_choices = [r["service"] for r in summary_rows]
                picked = filter_cols[0].multiselect(
                    "Filter services (empty = all)", svc_choices, key="conn_svc_filter",
                )
                only_with_incidents = filter_cols[1].checkbox(
                    "Only with incidents", value=True, key="conn_only_bad",
                )
                dep_filter = filter_cols[2].multiselect(
                    "Dependencies", ["kafka", "mongo", "elasticsearch"], key="conn_dep_filter",
                )

                view = [
                    r for r in summary_rows
                    if (not picked or r["service"] in picked)
                    and (not only_with_incidents or r["incidents"] > 0)
                ]

                if view:
                    import pandas as pd
                    df = pd.DataFrame(view).sort_values(
                        by=["incidents", "total_downtime_s", "service"],
                        ascending=[False, False, True],
                    )

                    def _row_style(r):
                        bad = (r.get("incidents") or 0) > 0
                        return [
                            "background-color: #ffe5e5; color:#7a0000" if bad else ""
                            for _ in r
                        ]

                    st.dataframe(
                        df.style.apply(_row_style, axis=1).format(
                            {"total_downtime_s": "{:.1f}"},
                        ),
                        hide_index=True,
                        use_container_width=True,
                    )
                else:
                    st.success("No connectivity incidents in scope.")

                # Detalle expandible por servicio con incidentes.
                detail_view = [
                    r for r in view
                    if r["incidents"] > 0 and (
                        not dep_filter
                        or any(d in r["deps_affected"] for d in dep_filter)
                    )
                ]
                if detail_view:
                    st.markdown("---")
                    st.markdown("##### Detail per service")
                    for r in detail_view:
                        svc = r["service"]
                        incidents = per_svc_incidents.get(svc, [])
                        if dep_filter:
                            incidents = [i for i in incidents if i["dependency"] in dep_filter]
                        if not incidents:
                            continue
                        headline = (
                            f"**{svc}** — {len(incidents)} incidents · "
                            f"{r['deps_affected']} · "
                            f"{r['total_downtime_s']}s total"
                        )
                        with st.expander(headline, expanded=False):
                            import pandas as pd
                            df_inc = pd.DataFrame(incidents)
                            st.dataframe(df_inc, hide_index=True, use_container_width=True)

                # Gantt agregado (todos los runs, mismo widget que la antigua tab).
                with st.expander("Gantt — all runs in registry", expanded=False):
                    items = connectivity_gantt(all_rows)
                    if dep_filter:
                        items = [i for i in items if i["dependency"] in dep_filter]
                    if picked:
                        items = [i for i in items if i["service_name"] in picked]
                    if not items:
                        st.info("No incidents to plot (con los filtros actuales).")
                    else:
                        fig = px.timeline(
                            items,
                            x_start="down_at",
                            x_end="up_at",
                            y="dependency",
                            color="service_name",
                            hover_data=["duration_seconds", "service_name", "run_id"],
                        )
                        fig.update_yaxes(autorange="reversed")
                        st.plotly_chart(fig, use_container_width=True)

        with tab_baseline:
            st.subheader("Baseline Editor — pin or ban templates")
            st.caption(
                "**pinned**: template SIEMPRE forma parte del baseline (no se marca como new). "
                "**banned**: template NO debe aparecer; si aparece se reporta como policy_violation."
            )
            if not has_registry:
                _no_registry_notice(registry_dir)
            elif not services:
                _no_runs_notice()
            else:
                # Pickers inline — el editor es per-cohorte (service, scenario)
                # por diseño, así que aquí sí necesitamos elegir explícitamente.
                be_cols = st.columns([2, 2, 3])
                service = be_cols[0].selectbox(
                    "Service", services, index=0, key="baseline_editor_svc",
                )
                scenarios = list_scenarios(registry_table, service) if registry_table is not None else []
                scenario = be_cols[1].selectbox(
                    "Scenario", scenarios or [""], index=0, key="baseline_editor_scn",
                )
                runs = filter_runs(registry_table, service, scenario) if registry_table is not None else []
                baseline_for_cohort = (
                    load_baseline_for(baseline_table, service, scenario or "")
                    if baseline_table is not None else {}
                )
                if not runs:
                    st.info(f"No runs for {service}/{scenario or '(none)'}.")
                    st.stop()
                run_id = be_cols[2].selectbox(
                    "Reference run", [str(r.get("run_id")) for r in runs],
                    index=len(runs) - 1, key="baseline_editor_run",
                )
                run_dir = next(
                    (Path(r.get("run_dir") or "") for r in runs if str(r.get("run_id")) == run_id),
                    None,
                )
                if run_dir is None:
                    st.warning("Selected run is no longer in the registry; pick another.")
                    st.stop()
                templates_path = run_dir / "templates.parquet"
                run_templates: list[dict[str, object]] = []
                if templates_path.exists():
                    run_templates = pq.read_table(templates_path).to_pylist()

                overrides_data = load_overrides(registry_dir)
                current = {
                    k.split("::", 2)[2]: v
                    for k, v in (overrides_data.get("overrides") or {}).items()
                    if k.startswith(f"{service}::{scenario or ''}::")
                }

                # Universo de templates: los del run + los del baseline + los ya con override
                universe: dict[str, dict[str, object]] = {}
                for row in run_templates:
                    tid = str(row.get("template_id"))
                    universe.setdefault(tid, {
                        "template_id": tid,
                        "severity": row.get("severity_text"),
                        "issue_kind": row.get("issue_kind"),
                        "in_run_count": int(row.get("event_count") or 0),
                        "in_baseline": tid in baseline_for_cohort,
                        "normalized": row.get("normalized_template"),
                    })
                for tid, base in baseline_for_cohort.items():
                    if tid not in universe:
                        universe[tid] = {
                            "template_id": tid,
                            "severity": base.get("severity_text"),
                            "issue_kind": base.get("issue_kind"),
                            "in_run_count": 0,
                            "in_baseline": True,
                            "normalized": base.get("normalized_template"),
                        }
                for tid in current:
                    if tid not in universe:
                        universe[tid] = {
                            "template_id": tid,
                            "severity": None,
                            "issue_kind": None,
                            "in_run_count": 0,
                            "in_baseline": False,
                            "normalized": "(template no presente en run ni baseline)",
                        }

                filter_text = st.text_input(
                    "Filter (substring on template id / normalized text)",
                    value="",
                    key="baseline_editor_filter",
                )
                limit = st.slider("Max rows", 20, 500, 100, 20, key="baseline_editor_limit")

                rows = list(universe.values())
                if filter_text:
                    f = filter_text.lower()
                    rows = [r for r in rows if f in str(r.get("template_id","")).lower()
                            or f in str(r.get("normalized","")).lower()]
                rows.sort(key=lambda r: (-int(r.get("in_run_count") or 0), str(r.get("template_id"))))
                rows = rows[:limit]

                pending_changes: dict[str, tuple[str | None, str]] = {}
                with st.form("baseline_overrides_form", clear_on_submit=False):
                    for row in rows:
                        tid = str(row["template_id"])
                        existing = current.get(tid, {})
                        existing_decision = existing.get("decision")
                        cols = st.columns([3, 1, 1, 2])
                        cols[0].markdown(
                            f"`{tid[:16]}…` — `{row.get('severity') or '?'}` / `{row.get('issue_kind') or '?'}`  \n"
                            f"<span style='font-size:11px;color:#888'>{(row.get('normalized') or '')[:140]}</span>",
                            unsafe_allow_html=True,
                        )
                        cols[1].markdown(f"run: **{row.get('in_run_count',0)}**  \nbase: **{'yes' if row.get('in_baseline') else 'no'}**")
                        decision = cols[2].selectbox(
                            "decision",
                            options=["(no override)", "pinned", "banned"],
                            index=(0 if not existing_decision else (1 if existing_decision == "pinned" else 2)),
                            key=f"dec_{tid}",
                            label_visibility="collapsed",
                        )
                        reason = cols[3].text_input(
                            "reason",
                            value=existing.get("reason", ""),
                            key=f"reason_{tid}",
                            label_visibility="collapsed",
                            placeholder="reason (optional)",
                        )
                        new_decision = None if decision == "(no override)" else decision
                        if new_decision != existing_decision or (new_decision and reason != existing.get("reason", "")):
                            pending_changes[tid] = (new_decision, reason)

                    set_by = st.text_input("set_by", value=os.environ.get("USER", "cbernal"), key="baseline_set_by")
                    submitted = st.form_submit_button(f"Save {len(pending_changes)} change(s)")

                if submitted and pending_changes:
                    for tid, (decision, reason) in pending_changes.items():
                        set_override(
                            overrides_data,
                            service=service,
                            scenario=scenario or "",
                            template_id=tid,
                            decision=decision,
                            reason=reason,
                            set_by=set_by,
                        )
                    path = save_overrides(registry_dir, overrides_data)
                    st.success(f"Saved {len(pending_changes)} override(s) -> {path}")
                    _load_registry_cached.clear()  # forzar re-lectura en siguientes navegaciones
                    st.rerun()
                elif submitted:
                    st.info("No changes to save.")

                st.markdown("---")
                st.markdown("##### Current overrides for this cohort")
                cohort_overrides = list_overrides_for(overrides_data, service=service, scenario=scenario or "")
                if not cohort_overrides:
                    st.caption("(none)")
                else:
                    st.dataframe(cohort_overrides, hide_index=True, use_container_width=True)

        with tab_files:
            st.subheader("Persistence layout")
            registry_path_local = registry_dir / "registry.parquet"
            baseline_path_local = registry_dir / "baseline.parquet"
            template_reg_path = registry_dir / "template_registry.parquet"
            state_path = registry_dir / "index_state.json"
            # Files & Registry siempre se muestra: aporta info útil aunque no haya
            # registry todavía (paths esperados, tamaños, comando para poblar).
            if not has_registry:
                st.info("Aún no existe `registry.parquet`. La tabla de abajo refleja el estado actual.")
            st.markdown(
                f"""
    **Registry directory:** `{registry_dir}`

    | File | Purpose | Size |
    |---|---|---|
    | `registry.parquet` | one row per scan run | {_size_str(registry_path_local)} |
    | `template_registry.parquet` | one row per (service, template_id) | {_size_str(template_reg_path)} |
    | `baseline.parquet` | statistical baseline per (service, scenario, template_id) | {_size_str(baseline_path_local)} |
    | `index_state.json` | incremental state of last `logs-reaper index` run | {_size_str(state_path)} |
    """
            )
            st.markdown("**Scan output per run** lives in `out/<service>/<RUN_ID>/`:")
            st.code(
                "events.parquet     # columnar event store (zstd + dict)\n"
                "templates.parquet  # aggregated templates with severity/issue_kind\n"
                "errors.parquet     # error rows surfaced for the report\n"
                "run.json           # metadata: counts, durations, connectivity_timeline\n"
                "summary.json       # short numerical summary\n"
                "report.md          # human-readable markdown report\n"
                "diff.parquet+json  # written when `logs-reaper diff` runs",
                language="text",
            )
            st.subheader("Current snapshot")
            # Reusamos `all_rows` calculado en setup; evita re-materializar el registry.
            all_runs_rows = all_rows
            cols = st.columns(4)
            cols[0].metric("Runs", len(all_runs_rows))
            cols[1].metric(
                "Green runs",
                sum(1 for r in all_runs_rows if r.get("status") == "green"),
            )
            cols[2].metric(
                "Red runs",
                sum(1 for r in all_runs_rows if r.get("status") == "red"),
            )
            cols[3].metric(
                "Baseline rows",
                baseline_table.num_rows if baseline_table is not None else 0,
            )
            if all_runs_rows:
                st.dataframe(
                    [
                        {
                            "run_id": r.get("run_id"),
                            "service": r.get("service_name"),
                            "scenario": r.get("scenario"),
                            "status": r.get("status"),
                            "events": r.get("event_count"),
                            "templates": r.get("template_count"),
                            "errors": r.get("error_count"),
                            "incidents": r.get("connectivity_incident_count"),
                            "created_at": r.get("created_at"),
                            "run_dir": r.get("run_dir"),
                        }
                        for r in all_runs_rows
                    ],
                    hide_index=True,
                    use_container_width=True,
                )

        with tab_heatmap:
            st.subheader("Template × Run heatmap (z-score vs baseline)")
            st.caption("Picker inline; el heatmap es por servicio porque las plantillas son específicas de cada uno.")
            if not has_registry:
                _no_registry_notice(registry_dir)
            elif not services:
                _no_runs_notice()
            else:
                service_h, scenario_h, runs_h, bfc_h = _inline_service_picker(
                    services, registry_table, baseline_table, "heatmap",
                )
                if not runs_h:
                    st.info(f"No runs for {service_h}/{scenario_h or '(none)'}.")
                else:
                    top_n = st.slider("Top N templates", min_value=10, max_value=200, value=50, step=10, key="heatmap_topn")
                    data = heatmap_matrix(runs_h, baseline_for_cohort=bfc_h, top_n=top_n)
                    if not data["template_ids"]:
                        st.info("No templates to plot.")
                    else:
                        z_grid = [[(0.0 if v is None else float(v)) for v in row] for row in data["z_scores"]]
                        fig = go.Figure(
                            data=go.Heatmap(
                                z=z_grid,
                                x=data["run_ids"],
                                y=[tid[:16] for tid in data["template_ids"]],
                                colorscale="RdYlGn_r",
                                zmid=0.0,
                                hovertemplate="run=%{x}<br>template=%{y}<br>z=%{z:.2f}<extra></extra>",
                            )
                        )
                        fig.update_layout(height=max(400, 16 * len(data["template_ids"])))
                        st.plotly_chart(fig, use_container_width=True)

        with tab_novelty:
            st.subheader("Novelty — fraction of templates new vs. previous N runs")
            st.caption("Resumen cross-service (último run de cada servicio) + curva detallada del seleccionado.")
            if not has_registry:
                _no_registry_notice(registry_dir)
            elif not services:
                _no_runs_notice()
            else:
                window = st.slider("Window (runs)", min_value=2, max_value=20, value=5, key="nov_window")

                # Resumen cross-service: una curva por servicio, tomamos el último valor.
                cross_rows: list[dict] = []
                for svc in sorted(services):
                    svc_runs = filter_runs(registry_table, svc, None) if registry_table is not None else []
                    if not svc_runs:
                        continue
                    curve = novelty_curve(svc_runs, window=window)
                    last = (curve.get("rows") or [{}])[-1]
                    cross_rows.append({
                        "service": svc,
                        "last_run_id": last.get("run_id"),
                        "novelty_fraction": float(last.get("novelty_fraction") or 0.0),
                        "novel_count": int(last.get("novel_count") or 0),
                        "templates_in_run": int(last.get("templates_in_run") or 0),
                    })
                if cross_rows:
                    import pandas as pd
                    df = pd.DataFrame(cross_rows).sort_values(
                        by=["novelty_fraction", "novel_count"], ascending=[False, False],
                    )

                    def _row_style(r):
                        bad = (r.get("novelty_fraction") or 0.0) >= 0.5 or (r.get("novel_count") or 0) > 0
                        return [
                            "background-color: #ffe5e5; color:#7a0000" if bad else ""
                            for _ in r
                        ]

                    st.markdown("##### Cross-service novelty (latest run per service)")
                    st.dataframe(
                        df.style.apply(_row_style, axis=1).format({"novelty_fraction": "{:.2f}"}),
                        hide_index=True, use_container_width=True,
                    )

                # Picker para detalle.
                st.markdown("---")
                st.markdown("##### Detail per service")
                service_n, scenario_n, runs_n, _ = _inline_service_picker(
                    services, registry_table, baseline_table, "novelty",
                )
                if not runs_n:
                    st.info(f"No runs for {service_n}/{scenario_n or '(none)'}.")
                else:
                    curve = novelty_curve(runs_n, window=window)
                    rows_n = curve["rows"]
                    if not rows_n:
                        st.info("No data.")
                    else:
                        fig = px.line(
                            x=[row["run_id"] for row in rows_n],
                            y=[row["novelty_fraction"] for row in rows_n],
                            labels={"x": "run_id", "y": "novelty fraction"},
                            markers=True,
                        )
                        st.plotly_chart(fig, use_container_width=True)
                        st.dataframe(rows_n, hide_index=True)

        with tab_survival:
            st.subheader("Kaplan-Meier — time-to-first-code-error per service instance")
            st.caption("Resumen del último run por servicio. Selecciona uno abajo para detalle.")
            if not has_registry:
                _no_registry_notice(registry_dir)
            elif not latest_by_svc:
                _no_runs_notice()
            else:
                # Resumen: para cada svc, conteo de instancias y cuántas sobrevivieron.
                summary_rows: list[dict] = []
                for svc, run in sorted(latest_by_svc.items()):
                    run_dir_s = Path(run.get("run_dir") or "")
                    if not run_dir_s.exists():
                        continue
                    surv = survival_post_boot(run_dir_s) or []
                    if not surv:
                        continue
                    with_error = [r for r in surv if r.get("first_code_ts") is not None]
                    summary_rows.append({
                        "service": svc,
                        "instances": len(surv),
                        "with_first_error": len(with_error),
                        "survived": len(surv) - len(with_error),
                        "run_id": run.get("run_id"),
                    })
                if summary_rows:
                    import pandas as pd
                    df = pd.DataFrame(summary_rows).sort_values(
                        by=["with_first_error", "instances"], ascending=[False, False],
                    )

                    def _row_style(r):
                        bad = (r.get("with_first_error") or 0) > 0
                        return [
                            "background-color: #ffe5e5; color:#7a0000" if bad else ""
                            for _ in r
                        ]

                    st.dataframe(
                        df.style.apply(_row_style, axis=1),
                        hide_index=True, use_container_width=True,
                    )

                st.markdown("---")
                st.markdown("##### Detail per service")
                service_s, scenario_s, runs_s, _ = _inline_service_picker(
                    services, registry_table, baseline_table, "survival",
                )
                if runs_s:
                    run_id = st.selectbox(
                        "Run", [str(r.get("run_id")) for r in runs_s], index=len(runs_s) - 1, key="survival_run",
                    )
                    run_dir = next(
                        (Path(r.get("run_dir") or "") for r in runs_s if str(r.get("run_id")) == run_id),
                        None,
                    )
                    if run_dir is None:
                        st.warning("Selected run is no longer present in the registry.")
                    else:
                        survival = survival_post_boot(run_dir)
                        if not survival:
                            st.info("No service instances detected.")
                        else:
                            st.dataframe(survival, hide_index=True, use_container_width=True)

        with tab_burndown:
            st.subheader("Regression burn-down (new vs fixed per run)")
            st.caption("Por servicio: cuántas regresiones de plantilla aparecen vs. se arreglan run-a-run.")
            if not has_registry:
                _no_registry_notice(registry_dir)
            elif not services:
                _no_runs_notice()
            else:
                service_b, scenario_b, runs_b, bfc_b = _inline_service_picker(
                    services, registry_table, baseline_table, "burndown",
                )
                if not runs_b:
                    st.info(f"No runs for {service_b}/{scenario_b or '(none)'}.")
                else:
                    burn = regression_burndown(runs_b, baseline_for_cohort=bfc_b)
                    if not burn:
                        st.info("No data.")
                    else:
                        fig = go.Figure()
                        fig.add_bar(name="new", x=[b["run_id"] for b in burn], y=[b["new_regressions"] for b in burn])
                        fig.add_bar(name="fixed", x=[b["run_id"] for b in burn], y=[-b["fixed_regressions"] for b in burn])
                        fig.update_layout(barmode="relative", height=400)
                        st.plotly_chart(fig, use_container_width=True)
                        st.dataframe(burn, hide_index=True, use_container_width=True)

        with tab_drill:
            st.subheader("Templates")
            st.caption("Templates parquet del run elegido. Pulsa abajo para elegir servicio/run.")
            if not has_registry:
                _no_registry_notice(registry_dir)
            elif not services:
                _no_runs_notice()
            else:
                service_d, scenario_d, runs_d, _ = _inline_service_picker(
                    services, registry_table, baseline_table, "drill",
                )
                if not runs_d:
                    st.info(f"No runs for {service_d}/{scenario_d or '(none)'}.")
                else:
                    run_id = st.selectbox(
                        "Run", [str(r.get("run_id")) for r in runs_d], key="drill_run",
                        index=len(runs_d) - 1,
                    )
                    run_dir = next(
                        (Path(r.get("run_dir") or "") for r in runs_d if str(r.get("run_id")) == run_id),
                        None,
                    )
                    if run_dir is None:
                        st.warning("Selected run is no longer present in the registry.")
                    else:
                        templates_path = run_dir / "templates.parquet"
                        if templates_path.exists():
                            templates = pq.read_table(templates_path).to_pylist()
                            # Filtro de texto para no abrumar.
                            f = st.text_input(
                                "Filter template id / normalized text", value="", key="drill_filter",
                            )
                            if f:
                                fl = f.lower()
                                templates = [
                                    t for t in templates
                                    if fl in str(t.get("template_id", "")).lower()
                                    or fl in str(t.get("normalized_template", "")).lower()
                                ]
                            st.dataframe(templates[:500], hide_index=True, use_container_width=True)
                            if len(templates) > 500:
                                st.caption(f"Showing first 500 of {len(templates)} rows. Refine the filter to narrow.")
                        else:
                            st.info("templates.parquet not present in this run yet.")


    # Fragment-scoped refresh: on every tick `_content` re-runs *only the
    # main pane*, keeping sidebar / expander state intact. The cadence
    # here is the safety fallback — manual refresh via the button is the
    # primary path.
    def _content_with_stamp() -> None:
        # Update the "last refresh" timestamp on every actual rerun so the
        # sidebar widget shows the truth, not the time of the initial load.
        st.session_state[LAST_REFRESH_KEY] = _time.time()
        _content()

    st.fragment(run_every=f"{AUTO_REFRESH_INTERVAL_SECS}s")(_content_with_stamp)()

    # Sidebar counters live outside the fragment (Streamlit forbids
    # sidebar writes from inside one). _content() publishes them via
    # session_state on each tick; we read+render here.
    if st.session_state.get("_sidebar_has_registry"):
        st.sidebar.markdown(
            f"**Services indexed:** {st.session_state.get('_sidebar_services_indexed', 0)}  \n"
            f"**Total runs:** {st.session_state.get('_sidebar_total_runs', 0)}"
        )
    else:
        st.sidebar.info(
            "Sin registry indexado todavía. Las tabs históricas se llenan al indexar la captura."
        )


if __name__ == "__main__":
    main()
