# Version Comparison

`logs-reaper compare` compares two scan output directories:

```bash
python3 -m logs_reaper compare --left out/RUN_A --right out/RUN_B --out out/diff.md
```

The command reads:

- `templates.parquet`
- `run.json`

It emits:

- Markdown diff at `--out` when the path ends in `.md`, otherwise `diff.md` in the output directory.
- JSON diff next to the Markdown report.

## Signals

- New templates in the right run.
- Fixed templates that disappeared from the right run.
- Regressions: new right-side templates with error severity or exception kind.
- Fixed errors: left-side error templates absent from the right run.
- Frequency increases for common templates.
- Library metadata differences from `--lib name=version`.

## CI Usage

Use `summary.json` and `diff.json` for machine checks. A practical initial gate is:

- Fail on any `regression_count > 0`.
- Warn on `frequency_increase_count > 0`.
- Keep `known-noise` visible but non-blocking.
