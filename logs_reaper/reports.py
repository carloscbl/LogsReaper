from __future__ import annotations

from typing import Any


def render_scan_report(
    run_metadata: dict[str, Any],
    summary: dict[str, Any],
    templates: list[dict[str, Any]],
    errors: list[dict[str, Any]],
) -> str:
    lines = [
        f"# LogsReaper Report: {run_metadata['run_id']}",
        "",
        "## Summary",
        "",
        f"- Events: {summary['event_count']}",
        f"- Templates: {summary['template_count']}",
        f"- Errors: {summary['error_count']}",
        f"- Engine: {run_metadata.get('engine') or 'unknown'}",
        f"- Hash algorithm: {run_metadata['hash_algorithm']}",
        f"- Files: {run_metadata['file_count']}",
        f"- Input GB: {_format_float(summary.get('input_gigabytes'))}",
        f"- Duration seconds: {_format_float(summary.get('scan_duration_seconds'))}",
        f"- Throughput GB/s: {_format_float(summary.get('throughput_gb_per_second'))}",
        f"- Events/s: {_format_float(summary.get('events_per_second'))}",
        "",
    ]
    lines.extend(_invocation_section(run_metadata))
    lines.extend(_instances_section(run_metadata))
    lines.extend(["## Severity Counts", ""])
    lines.extend(_bullet_map(summary.get("severity_counts") or {}))
    lines.extend(["", "## Classification Counts", ""])
    lines.extend(_bullet_map(summary.get("classification_counts") or {}))
    issue_counts = summary.get("issue_kind_counts") or {}
    if issue_counts:
        lines.extend(["", "## Issue Kind Counts (templates)", ""])
        lines.extend(_bullet_map(issue_counts))
    focus = (run_metadata.get("focus") or "both").lower()
    if focus in {"both", "code"}:
        lines.extend(_focus_section(
            title="Code Issues (engineer focus)",
            description="Templates that look like bugs in our own code: tracebacks, typed Python exceptions, asserts, etc.",
            errors=errors,
            templates=templates,
            kind="code",
        ))
    if focus in {"both", "infra"}:
        lines.extend(_focus_section(
            title="Infrastructure Issues (ops focus)",
            description="Templates that look like external dependency problems (kafka, mongo, network, DNS) rather than code bugs.",
            errors=errors,
            templates=templates,
            kind="infra",
        ))
        lines.extend(_connectivity_section(run_metadata.get("connectivity_timeline") or {}))
    lines.extend(["", "## All Error Templates", ""])
    lines.extend(_template_table(errors[:30], include_reason=True))
    lines.extend(["", "## Top Templates", ""])
    lines.extend(_template_table(templates[:20], include_reason=False))
    lines.append("")
    return "\n".join(lines)


def _focus_section(*, title: str, description: str, errors: list[dict[str, Any]], templates: list[dict[str, Any]], kind: str) -> list[str]:
    lines = ["", f"## {title}", "", description, ""]
    relevant_errors = [row for row in errors if row.get("issue_kind") == kind]
    if relevant_errors:
        lines.append(f"### Error templates ({len(relevant_errors)})")
        lines.append("")
        lines.extend(_template_table(relevant_errors[:20], include_reason=True))
    else:
        lines.append("- No error templates matched this focus.")
    warning_templates = [
        row
        for row in templates
        if row.get("issue_kind") == kind
        and str(row.get("severity_text") or "").upper() == "WARNING"
    ][:20]
    if warning_templates:
        lines.extend(["", f"### Notable warnings ({len(warning_templates)})", ""])
        lines.extend(_template_table(warning_templates, include_reason=False))
    lines.append("")
    return lines


def _connectivity_section(timeline: dict[str, Any]) -> list[str]:
    if not timeline:
        return []
    lines = ["", "### Connectivity timeline", "",
             "Down/up windows detected from event bodies. A closed interval (`up_at` set) means the dependency recovered; an open interval means we never saw a recovery in the analysed window.",
             ""]
    saw_any = False
    for service, info in timeline.items():
        incidents = info.get("incidents") or []
        if not incidents and (info.get("state") in (None, "unknown")):
            continue
        saw_any = True
        state = info.get("state") or "unknown"
        lines.append(f"**{service}** — current state: `{state}`, down events: {info.get('down_events', 0)}, up events: {info.get('up_events', 0)}")
        if incidents:
            lines.append("")
            lines.append("| # | down_at | up_at | duration | down_events | first sample |")
            lines.append("| ---: | --- | --- | --- | ---: | --- |")
            for index, incident in enumerate(incidents[:20], start=1):
                duration = incident.get("duration_seconds")
                duration_str = "ongoing" if duration is None else _format_duration(duration)
                lines.append(
                    "| {n} | {d} | {u} | {dur} | {count} | {sample} |".format(
                        n=index,
                        d=incident.get("down_at") or "",
                        u=incident.get("up_at") or "(no recovery)",
                        dur=duration_str,
                        count=incident.get("down_events") or 0,
                        sample=_escape(incident.get("first_sample") or ""),
                    )
                )
        lines.append("")
    if not saw_any:
        lines.append("- No connectivity transitions detected.")
        lines.append("")
    return lines


def _format_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        minutes, secs = divmod(seconds, 60)
        return f"{int(minutes)}m{int(secs):02d}s"
    hours, rem = divmod(seconds, 3600)
    minutes, _ = divmod(rem, 60)
    return f"{int(hours)}h{int(minutes):02d}m"


def render_compare_report(payload: dict[str, Any]) -> str:
    lines = [
        f"# LogsReaper Diff: {payload['left_run_id']} -> {payload['right_run_id']}",
        "",
        "## Summary",
        "",
        f"- New templates: {payload['new_template_count']}",
        f"- Fixed templates: {payload['fixed_template_count']}",
        f"- Regressions: {payload['regression_count']}",
        f"- Fixed errors: {payload['fixed_error_count']}",
        f"- Frequency increases: {payload['frequency_increase_count']}",
        "",
        "## Regressions",
        "",
    ]
    lines.extend(_template_table(payload["regressions"], include_reason=True))
    lines.extend(["", "## Fixed Errors", ""])
    lines.extend(_template_table(payload["fixed_errors"], include_reason=False))
    lines.extend(["", "## Frequency Increases", ""])
    if payload["frequency_increases"]:
        lines.append("| template_id | service | severity | left | right | template |")
        lines.append("| --- | --- | --- | ---: | ---: | --- |")
        for row in payload["frequency_increases"][:20]:
            lines.append(
                "| {template_id} | {service_name} | {severity_text} | {left_count} | {right_count} | {template} |".format(
                    template_id=row["template_id"],
                    service_name=row.get("service_name") or "",
                    severity_text=row.get("severity_text") or "",
                    left_count=row["left_count"],
                    right_count=row["right_count"],
                    template=_escape(row.get("normalized_template")),
                )
            )
    else:
        lines.append("No frequency increases matched the configured threshold.")
    lines.append("")
    return "\n".join(lines)


def _invocation_section(run_metadata: dict[str, Any]) -> list[str]:
    lines = ["## Invocation", ""]
    command = run_metadata.get("invocation_command")
    if command:
        lines.extend(["- Command:", "", "```", str(command), "```", ""])
    autodiscovery = run_metadata.get("autodiscovery") or {}
    if autodiscovery:
        mode = autodiscovery.get("mode")
        if mode:
            lines.append(f"- Source mode: {mode}")
        for field, label in (
            ("container_name", "Container name"),
            ("container_id", "Container id"),
            ("container_started_at", "Container started at"),
            ("status", "Snapshot status"),
            ("fingerprint", "Snapshot fingerprint"),
            ("captured_log_path", "Captured log path"),
        ):
            value = autodiscovery.get(field)
            if value:
                lines.append(f"- {label}: `{value}`")
    input_globs = run_metadata.get("input_globs") or []
    if input_globs:
        lines.append(f"- Input globs: {', '.join(f'`{glob}`' for glob in input_globs)}")
    input_files = run_metadata.get("input_files") or []
    if input_files:
        lines.append(f"- Input files ({len(input_files)}):")
        for path in input_files[:20]:
            lines.append(f"    - `{path}`")
        if len(input_files) > 20:
            lines.append(f"    - ... and {len(input_files) - 20} more")
    lines.append("")
    return lines


def _instances_section(run_metadata: dict[str, Any]) -> list[str]:
    info = run_metadata.get("instances") or {}
    if not info:
        return []
    lines = ["## Service Instances", ""]
    spec = info.get("spec") or "last"
    detected = info.get("detected") or []
    kept_seqs = info.get("kept_seqs")
    filter_active = info.get("filter_active")
    dropped = info.get("dropped_event_count") or 0
    lines.append(f"- Filter spec: `{spec}`")
    lines.append(f"- Detected boots: {len(detected)}")
    if filter_active:
        kept = ", ".join(str(seq) for seq in (kept_seqs or [])) or "none"
        lines.append(f"- Kept instance seq(s): {kept}")
        lines.append(f"- Dropped events from older instances: {dropped}")
    else:
        lines.append("- Kept instance seq(s): all")
        if dropped:
            lines.append(f"- Dropped events: {dropped}")
    tail_anchor = info.get("tail_anchor_offset")
    total_bytes = info.get("total_input_bytes")
    parsed_bytes = info.get("parsed_input_bytes")
    if tail_anchor is not None and total_bytes:
        skipped = total_bytes - (parsed_bytes or 0)
        pct = (skipped / total_bytes * 100.0) if total_bytes else 0.0
        lines.append(
            f"- Reverse-scan tail anchor: byte {tail_anchor:,} of {total_bytes:,} "
            f"(skipped {skipped:,} bytes / {pct:.2f}% of the file before parsing)"
        )
    if not detected:
        lines.append("- No service boot markers were detected; events belong to instance 0 (pre-boot logs).")
        lines.append("")
        return lines

    # When the filter dropped everything except a small set, list exactly those (that's the
    # only set the analyst actually cares about for this run). Otherwise (spec='all') the full
    # table can be in the hundreds-of-thousands of rows for a long log; surface only the top
    # five by event_count plus a "+ N more" footer. The complete list is always preserved in
    # run.json under `instances.detected`, so downstream tooling stays unaffected.
    kept_set = set(kept_seqs or [])
    if filter_active and kept_set:
        rows_to_show = [item for item in detected if item.get("seq") in kept_set]
        truncation_note: str | None = None
    else:
        ranked = sorted(detected, key=lambda item: int(item.get("event_count") or 0), reverse=True)
        top_n = 5
        rows_to_show = ranked[:top_n]
        remaining = len(detected) - len(rows_to_show)
        truncation_note = (
            f"- Showing top {len(rows_to_show)} by event_count of {len(detected)} detected boots "
            f"(see run.json#instances.detected for the full list, +{remaining} more)."
            if remaining > 0
            else None
        )

    if truncation_note:
        lines.append(truncation_note)
    lines.append("")
    lines.append("| seq | started_at | event_count | first_event_id |")
    lines.append("| ---: | --- | ---: | --- |")
    for item in rows_to_show:
        lines.append(
            "| {seq} | {started_at} | {event_count} | {first_event_id} |".format(
                seq=item.get("seq"),
                started_at=item.get("started_at") or "",
                event_count=item.get("event_count") or 0,
                first_event_id=item.get("first_event_id") or "",
            )
        )
    lines.append("")
    return lines


def _bullet_map(values: dict[str, Any]) -> list[str]:
    if not values:
        return ["- none"]
    return [f"- {key}: {value}" for key, value in sorted(values.items(), key=lambda item: str(item[0]))]


def _template_table(rows: list[dict[str, Any]], *, include_reason: bool) -> list[str]:
    if not rows:
        return ["No rows."]
    if include_reason:
        header = "| template_id | service | severity | class | count | reason | template |"
        sep = "| --- | --- | --- | --- | ---: | --- | --- |"
    else:
        header = "| template_id | service | severity | class | count | template |"
        sep = "| --- | --- | --- | --- | ---: | --- |"
    lines = [header, sep]
    for row in rows:
        common = {
            "template_id": row.get("template_id") or "",
            "service_name": row.get("service_name") or "",
            "severity_text": row.get("severity_text") or "",
            "classification": row.get("classification") or "",
            "event_count": row.get("event_count") or 0,
            "template": _escape(row.get("normalized_template")),
            "reason": _escape(row.get("reason") or row.get("classification_reason")),
        }
        if include_reason:
            lines.append(
                "| {template_id} | {service_name} | {severity_text} | {classification} | {event_count} | {reason} | {template} |".format(
                    **common
                )
            )
        else:
            lines.append(
                "| {template_id} | {service_name} | {severity_text} | {classification} | {event_count} | {template} |".format(
                    **common
                )
            )
    return lines


def _escape(value: Any) -> str:
    return str(value or "").replace("|", "\\|").replace("\n", " ")


def _format_float(value: Any) -> str:
    try:
        return f"{float(value):.6f}"
    except (TypeError, ValueError):
        return "0.000000"
