# Error Classification

LogsReaper v1 uses deterministic rules and baseline comparison. It does not call an online LLM.

## Classes

- `expected`: template exists in baseline or matches an explicit `expected` rule.
- `unexpected`: new error template, new exception type, or configured error severity without baseline/rule coverage.
- `known-noise`: explicit rule for accepted operational noise.
- `observed`: non-error template seen in the current run.
- `regression`: assigned by `compare` when an error template appears in the right run but not the left run.
- `fixed`: assigned by `compare` when an error template existed in the left run and disappeared in the right run.

## Rule Order

Rules are evaluated in this order:

1. `known_noise`
2. `expected`
3. baseline template match
4. unexpected error severity or exception
5. observed

Noise wins before expected because accepted noise should remain visible as noise in reports.

## YAML Rule Shape

```yaml
known_noise:
  - id: client_disconnect_broken_pipe
    severity: [WARNING, ERROR]
    template_regex: "(BrokenPipeError|connection reset)"
    reason: "Accepted client disconnect noise."

expected:
  - id: startup_log_level
    severity: [INFO]
    template_contains: "LogLevel:"
    reason: "Standard service startup logging."
```

Supported matchers:

- `template_id`
- `service_name`
- `severity`
- `error_kind`
- `exception_type`
- `template_contains`
- `template_regex`
- `body_contains`
- `body_regex`
- `parse_status`

## Frequency Regressions

`compare` flags frequency increases when a template exists in both runs and the right count crosses both thresholds:

- `right_count >= min_count`
- `right_count >= max(left_count * frequency_ratio, left_count + min_count)`

Defaults are `frequency_ratio=2.0` and `min_count=5`.
