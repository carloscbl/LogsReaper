# Log Enrichment Guide

LogsReaper v1 does not require service changes. If a service already emits structured logs, these optional fields improve grouping, comparison, and root-cause analysis.

## Recommended Fields

- `service.name` or `microservice`
- `service.instance.id`
- `process.pid`
- `process.name`
- `thread.name` or `threadName`
- `worker_id`
- `trace_id`
- `span_id`
- `container.id`
- `k8s.pod.name`
- `k8s.container.name`
- `server.kind`

## Example JSON Formatter Output

A typical structured logger emits these useful JSON keys, all of which LogsReaper consumes:

- `time`
- `level`
- `message`
- `microservice`
- `worker_id`
- `funcName`
- `threadName`

Text-formatted fallback logs are also supported when they include timestamp, level, thread, process, source location, function, and message.

## Minimal Change Policy

For v1, prefer scanning existing logs. Add fields only where missing context blocks debugging, such as distinguishing workers, processes, pods, or trace-correlated failures.

## PII Notes

LogsReaper normalizes templates and excludes raw records from dataset export by default. Original `body` values can still contain PII in Parquet, so raw log artifacts should stay in controlled storage.
