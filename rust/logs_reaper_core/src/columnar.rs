// Columnar (Arrow) builders for events and templates. The schema mirrors what the Python
// management layer expects in `EVENT_SCHEMA` / `TEMPLATE_SCHEMA` (logs_reaper/io.py). Fields that
// Python populates post-parse (service_instance_seq, service_instance_started_at, issue_kind,
// baseline_match overrides, classification reasons after rule application) are intentionally
// omitted here — Python re-builds the columns when it materialises the Parquet outputs.

use crate::{RustEventRow, RustTemplateRow};
use arrow::array::{
    ArrayRef, BooleanBuilder, Int64Builder, RecordBatch, StringBuilder, StringDictionaryBuilder,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use rayon::prelude::*;
use std::sync::Arc;

/// Columns that we dictionary-encode in the Arrow output. Empirically these have cardinality
/// orders of magnitude below the row count even on multi-GiB captures (body / normalized_template
/// ~ hundreds of uniques; service_name / severity_text / parse_format ~ single digits), so the
/// dictionary representation saves 90-99% of the bytes while staying lossless.
///
/// Parquet handles DICTIONARY-encoded columns natively; pyarrow regex/compute kernels operate on
/// dictionary arrays transparently — we don't pay the full string scan to do `match_substring`,
/// we pay the cost of the dictionary only.
fn dict_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        nullable,
    )
}

pub fn events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        dict_field("run_id", false),
        dict_field("source", false),
        Field::new("offset", DataType::Int64, false),
        Field::new("line_count", DataType::Int64, false),
        Field::new("timestamp", DataType::Utf8, true),
        dict_field("observed_timestamp", false),
        dict_field("severity_text", false),
        Field::new("severity_number", DataType::Int64, false),
        dict_field("parse_format", false),
        dict_field("parse_status", false),
        dict_field("thread_name", true),
        dict_field("process_name", true),
        Field::new("process_pid", DataType::Int64, true),
        dict_field("body", false),
        dict_field("normalized_template", false),
        dict_field("error_kind", false),
        dict_field("exception_type", true),
        dict_field("template_id", false),
        Field::new("raw_hash", DataType::Utf8, false),
        dict_field("classification", false),
        dict_field("classification_reason", true),
        dict_field("service_name", false),
        dict_field("service_instance_id", true),
        dict_field("worker_id", true),
        dict_field("server_kind", true),
        dict_field("trace_id", true),
        dict_field("span_id", true),
        dict_field("container_id", true),
        dict_field("k8s_pod_name", true),
        dict_field("k8s_container_name", true),
        dict_field("attributes_json", false),
        dict_field("resource_json", false),
        dict_field("raw", true),
    ]))
}

pub fn templates_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("template_id", DataType::Utf8, false),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("severity_number", DataType::Int64, false),
        Field::new("normalized_template", DataType::Utf8, false),
        Field::new("error_kind", DataType::Utf8, false),
        Field::new("exception_type", DataType::Utf8, true),
        Field::new("event_count", DataType::Int64, false),
        Field::new("first_seen", DataType::Utf8, true),
        Field::new("last_seen", DataType::Utf8, true),
        Field::new("example_event_id", DataType::Utf8, false),
        Field::new("parse_status", DataType::Utf8, false),
        Field::new("classification", DataType::Utf8, false),
        Field::new("classification_reason", DataType::Utf8, true),
        Field::new("baseline_match", DataType::Boolean, false),
    ]))
}

/// Build a single RecordBatch from a slice of rows. For multi-million-row inputs prefer
/// `events_to_batch_parallel` which slices the input across rayon workers and concatenates.
pub fn events_to_batch(rows: &[RustEventRow]) -> RecordBatch {
    events_to_batch_inner(rows)
}

/// Parallel version: split `rows` into N chunks (one per rayon worker), build a RecordBatch per
/// chunk on its own thread, then concatenate. For very large inputs this overlaps the costly
/// per-cell `StringBuilder::append_value` work across cores, often halving the time relative to
/// the single-threaded `events_to_batch` on >100 k row inputs.
pub fn events_to_batch_parallel(rows: &[RustEventRow]) -> RecordBatch {
    const MIN_PARALLEL_ROWS: usize = 50_000;
    let n_workers = rayon::current_num_threads().max(1);
    if rows.len() < MIN_PARALLEL_ROWS || n_workers <= 1 {
        return events_to_batch_inner(rows);
    }
    let chunk_size = (rows.len() / n_workers).max(1);
    let batches: Vec<RecordBatch> = rows
        .par_chunks(chunk_size)
        .map(events_to_batch_inner)
        .collect();
    if batches.len() == 1 {
        return batches.into_iter().next().unwrap();
    }
    let schema = batches[0].schema();
    concat_batches(&schema, &batches).expect("concat events batches")
}

/// Build a list of RecordBatches in parallel (one per rayon worker), without concatenating
/// them. The caller (Python) then composes a pyarrow.Table via `from_batches`, which is
/// zero-copy — avoiding both the sequential `concat_batches` cost and the temporary memory
/// spike that comes from holding the source rows plus the concatenated buffers simultaneously.
pub fn events_to_batches_parallel(rows: &[RustEventRow]) -> Vec<RecordBatch> {
    const MIN_PARALLEL_ROWS: usize = 50_000;
    if rows.is_empty() {
        return vec![events_to_batch_inner(rows)];
    }
    let n_workers = rayon::current_num_threads().max(1);
    if rows.len() < MIN_PARALLEL_ROWS || n_workers <= 1 {
        return vec![events_to_batch_inner(rows)];
    }
    let chunk_size = (rows.len() / n_workers).max(1);
    rows.par_chunks(chunk_size).map(events_to_batch_inner).collect()
}

fn events_to_batch_inner(rows: &[RustEventRow]) -> RecordBatch {
    let mut builder = EventsBatchBuilder::with_capacity(rows.len().max(64));
    for row in rows {
        builder.push_ref(row);
    }
    builder.finish()
}

#[allow(dead_code)]
fn events_to_batch_inner_legacy(rows: &[RustEventRow]) -> RecordBatch {
    let cap = rows.len();
    let mut event_id = StringBuilder::with_capacity(cap, cap * 32);
    let mut run_id = StringBuilder::with_capacity(cap, cap * 16);
    let mut source = StringBuilder::with_capacity(cap, cap * 32);
    let mut offset = Int64Builder::with_capacity(cap);
    let mut line_count = Int64Builder::with_capacity(cap);
    let mut timestamp = StringBuilder::with_capacity(cap, cap * 24);
    let mut observed_timestamp = StringBuilder::with_capacity(cap, cap * 24);
    let mut severity_text = StringBuilder::with_capacity(cap, cap * 6);
    let mut severity_number = Int64Builder::with_capacity(cap);
    let mut parse_format = StringBuilder::with_capacity(cap, cap * 4);
    let mut parse_status = StringBuilder::with_capacity(cap, cap * 4);
    let mut thread_name = StringBuilder::with_capacity(cap, cap * 8);
    let mut process_name = StringBuilder::with_capacity(cap, cap * 8);
    let mut process_pid = Int64Builder::with_capacity(cap);
    let mut body = StringBuilder::with_capacity(cap, cap * 64);
    let mut normalized_template = StringBuilder::with_capacity(cap, cap * 64);
    let mut error_kind = StringBuilder::with_capacity(cap, cap * 8);
    let mut exception_type = StringBuilder::with_capacity(cap, cap * 16);
    let mut template_id = StringBuilder::with_capacity(cap, cap * 32);
    let mut raw_hash = StringBuilder::with_capacity(cap, cap * 32);
    let mut classification = StringBuilder::with_capacity(cap, cap * 8);
    let mut classification_reason = StringBuilder::with_capacity(cap, cap * 16);
    let mut service_name = StringBuilder::with_capacity(cap, cap * 16);
    let mut service_instance_id = StringBuilder::with_capacity(cap, cap * 16);
    let mut worker_id = StringBuilder::with_capacity(cap, cap * 8);
    let mut server_kind = StringBuilder::with_capacity(cap, cap * 8);
    let mut trace_id = StringBuilder::with_capacity(cap, cap * 32);
    let mut span_id = StringBuilder::with_capacity(cap, cap * 16);
    let mut container_id = StringBuilder::with_capacity(cap, cap * 16);
    let mut k8s_pod_name = StringBuilder::with_capacity(cap, cap * 16);
    let mut k8s_container_name = StringBuilder::with_capacity(cap, cap * 16);
    let mut attributes_json = StringBuilder::with_capacity(cap, cap * 16);
    let mut resource_json = StringBuilder::with_capacity(cap, cap * 16);
    let mut raw = StringBuilder::with_capacity(cap, cap * 64);

    for row in rows {
        event_id.append_value(&row.event_id);
        run_id.append_value(&row.run_id);
        source.append_value(&row.source);
        offset.append_value(row.offset);
        line_count.append_value(row.line_count);
        append_opt(&mut timestamp, row.timestamp.as_deref());
        observed_timestamp.append_value(&row.observed_timestamp);
        severity_text.append_value(&row.severity_text);
        severity_number.append_value(row.severity_number);
        parse_format.append_value(&row.parse_format);
        parse_status.append_value(&row.parse_status);
        append_opt(&mut thread_name, row.thread_name.as_deref());
        append_opt(&mut process_name, row.process_name.as_deref());
        append_opt_i64(&mut process_pid, row.process_pid);
        body.append_value(&row.body);
        normalized_template.append_value(&row.normalized_template);
        error_kind.append_value(&row.error_kind);
        append_opt(&mut exception_type, row.exception_type.as_deref());
        template_id.append_value(&row.template_id);
        raw_hash.append_value(&row.raw_hash);
        classification.append_value(&row.classification);
        append_opt(&mut classification_reason, row.classification_reason.as_deref());
        service_name.append_value(&row.service_name);
        append_opt(&mut service_instance_id, row.service_instance_id.as_deref());
        append_opt(&mut worker_id, row.worker_id.as_deref());
        append_opt(&mut server_kind, row.server_kind.as_deref());
        append_opt(&mut trace_id, row.trace_id.as_deref());
        append_opt(&mut span_id, row.span_id.as_deref());
        append_opt(&mut container_id, row.container_id.as_deref());
        append_opt(&mut k8s_pod_name, row.k8s_pod_name.as_deref());
        append_opt(&mut k8s_container_name, row.k8s_container_name.as_deref());
        attributes_json.append_value(&row.attributes_json);
        resource_json.append_value(&row.resource_json);
        append_opt(&mut raw, row.raw.as_deref());
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(event_id.finish()),
        Arc::new(run_id.finish()),
        Arc::new(source.finish()),
        Arc::new(offset.finish()),
        Arc::new(line_count.finish()),
        Arc::new(timestamp.finish()),
        Arc::new(observed_timestamp.finish()),
        Arc::new(severity_text.finish()),
        Arc::new(severity_number.finish()),
        Arc::new(parse_format.finish()),
        Arc::new(parse_status.finish()),
        Arc::new(thread_name.finish()),
        Arc::new(process_name.finish()),
        Arc::new(process_pid.finish()),
        Arc::new(body.finish()),
        Arc::new(normalized_template.finish()),
        Arc::new(error_kind.finish()),
        Arc::new(exception_type.finish()),
        Arc::new(template_id.finish()),
        Arc::new(raw_hash.finish()),
        Arc::new(classification.finish()),
        Arc::new(classification_reason.finish()),
        Arc::new(service_name.finish()),
        Arc::new(service_instance_id.finish()),
        Arc::new(worker_id.finish()),
        Arc::new(server_kind.finish()),
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(container_id.finish()),
        Arc::new(k8s_pod_name.finish()),
        Arc::new(k8s_container_name.finish()),
        Arc::new(attributes_json.finish()),
        Arc::new(resource_json.finish()),
        Arc::new(raw.finish()),
    ];
    RecordBatch::try_new(events_schema(), arrays).expect("events batch")
}

pub fn templates_to_batch(rows: &[RustTemplateRow]) -> RecordBatch {
    let cap = rows.len();
    let mut template_id = StringBuilder::with_capacity(cap, cap * 32);
    let mut service_name = StringBuilder::with_capacity(cap, cap * 16);
    let mut severity_text = StringBuilder::with_capacity(cap, cap * 6);
    let mut severity_number = Int64Builder::with_capacity(cap);
    let mut normalized_template = StringBuilder::with_capacity(cap, cap * 64);
    let mut error_kind = StringBuilder::with_capacity(cap, cap * 8);
    let mut exception_type = StringBuilder::with_capacity(cap, cap * 16);
    let mut event_count = Int64Builder::with_capacity(cap);
    let mut first_seen = StringBuilder::with_capacity(cap, cap * 24);
    let mut last_seen = StringBuilder::with_capacity(cap, cap * 24);
    let mut example_event_id = StringBuilder::with_capacity(cap, cap * 32);
    let mut parse_status = StringBuilder::with_capacity(cap, cap * 4);
    let mut classification = StringBuilder::with_capacity(cap, cap * 8);
    let mut classification_reason = StringBuilder::with_capacity(cap, cap * 16);
    let mut baseline_match = BooleanBuilder::with_capacity(cap);

    for row in rows {
        template_id.append_value(&row.template_id);
        service_name.append_value(&row.service_name);
        severity_text.append_value(&row.severity_text);
        severity_number.append_value(row.severity_number);
        normalized_template.append_value(&row.normalized_template);
        error_kind.append_value(&row.error_kind);
        append_opt(&mut exception_type, row.exception_type.as_deref());
        event_count.append_value(row.event_count as i64);
        append_opt(&mut first_seen, row.first_seen.as_deref());
        append_opt(&mut last_seen, row.last_seen.as_deref());
        example_event_id.append_value(&row.example_event_id);
        parse_status.append_value(&row.parse_status);
        classification.append_value(&row.classification);
        append_opt(&mut classification_reason, row.classification_reason.as_deref());
        baseline_match.append_value(row.baseline_match);
    }

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(template_id.finish()),
        Arc::new(service_name.finish()),
        Arc::new(severity_text.finish()),
        Arc::new(severity_number.finish()),
        Arc::new(normalized_template.finish()),
        Arc::new(error_kind.finish()),
        Arc::new(exception_type.finish()),
        Arc::new(event_count.finish()),
        Arc::new(first_seen.finish()),
        Arc::new(last_seen.finish()),
        Arc::new(example_event_id.finish()),
        Arc::new(parse_status.finish()),
        Arc::new(classification.finish()),
        Arc::new(classification_reason.finish()),
        Arc::new(baseline_match.finish()),
    ];
    RecordBatch::try_new(templates_schema(), arrays).expect("templates batch")
}

#[inline]
fn append_opt(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

#[inline]
fn append_opt_i64(builder: &mut Int64Builder, value: Option<i64>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

type DictBuilder = StringDictionaryBuilder<Int32Type>;

/// Streaming builder for the events RecordBatch. Lets the caller push one parsed row at a
/// time, so the Vec<RustEventRow> intermediate is avoided entirely: each row is consumed by
/// `push` into Arrow builders and immediately dropped, halving the peak RSS on multi-GiB scans.
///
/// Low-cardinality columns use `StringDictionaryBuilder` which holds each unique string once
/// per chunk + an int32 per row. For columns like `body` (2.4k uniques out of 4M rows on a
/// 1 GiB capture) this turns ~460 MB into ~30 MB; for true low-card columns like
/// `severity_text` it turns ~40 MB into ~16 MB of indices + a handful of bytes of dictionary.
pub struct EventsBatchBuilder {
    event_id: StringBuilder,
    run_id: DictBuilder,
    source: DictBuilder,
    offset: Int64Builder,
    line_count: Int64Builder,
    timestamp: StringBuilder,
    observed_timestamp: DictBuilder,
    severity_text: DictBuilder,
    severity_number: Int64Builder,
    parse_format: DictBuilder,
    parse_status: DictBuilder,
    thread_name: DictBuilder,
    process_name: DictBuilder,
    process_pid: Int64Builder,
    body: DictBuilder,
    normalized_template: DictBuilder,
    error_kind: DictBuilder,
    exception_type: DictBuilder,
    template_id: DictBuilder,
    raw_hash: StringBuilder,
    classification: DictBuilder,
    classification_reason: DictBuilder,
    service_name: DictBuilder,
    service_instance_id: DictBuilder,
    worker_id: DictBuilder,
    server_kind: DictBuilder,
    trace_id: DictBuilder,
    span_id: DictBuilder,
    container_id: DictBuilder,
    k8s_pod_name: DictBuilder,
    k8s_container_name: DictBuilder,
    attributes_json: DictBuilder,
    resource_json: DictBuilder,
    raw: DictBuilder,
    len: usize,
}

impl EventsBatchBuilder {
    pub fn with_capacity(cap: usize) -> Self {
        let s = |bytes_per: usize| StringBuilder::with_capacity(cap, cap * bytes_per);
        // Dictionary builders only need to size the indices array up front; the dictionary grows
        // organically. `with_capacity` here is for the indices (one Int32 per row).
        let d = || StringDictionaryBuilder::<Int32Type>::with_capacity(cap, 32, 1024);
        Self {
            event_id: s(32),
            run_id: d(),
            source: d(),
            offset: Int64Builder::with_capacity(cap),
            line_count: Int64Builder::with_capacity(cap),
            timestamp: s(24),
            observed_timestamp: d(),
            severity_text: d(),
            severity_number: Int64Builder::with_capacity(cap),
            parse_format: d(),
            parse_status: d(),
            thread_name: d(),
            process_name: d(),
            process_pid: Int64Builder::with_capacity(cap),
            body: d(),
            normalized_template: d(),
            error_kind: d(),
            exception_type: d(),
            template_id: d(),
            raw_hash: s(32),
            classification: d(),
            classification_reason: d(),
            service_name: d(),
            service_instance_id: d(),
            worker_id: d(),
            server_kind: d(),
            trace_id: d(),
            span_id: d(),
            container_id: d(),
            k8s_pod_name: d(),
            k8s_container_name: d(),
            attributes_json: d(),
            resource_json: d(),
            raw: d(),
            len: 0,
        }
    }

    /// Consume a row, pushing its fields into the column builders and dropping it at the end.
    pub fn push(&mut self, row: RustEventRow) {
        self.push_ref(&row);
    }

    /// Push by reference; functionally equivalent to `push` but used by the legacy
    /// `events_to_batch` adapter that walks a borrowed slice of rows.
    pub fn push_ref(&mut self, row: &RustEventRow) {
        self.event_id.append_value(&row.event_id);
        self.run_id.append_value(&row.run_id);
        self.source.append_value(&row.source);
        self.offset.append_value(row.offset);
        self.line_count.append_value(row.line_count);
        append_opt(&mut self.timestamp, row.timestamp.as_deref());
        self.observed_timestamp.append_value(&row.observed_timestamp);
        self.severity_text.append_value(&row.severity_text);
        self.severity_number.append_value(row.severity_number);
        self.parse_format.append_value(&row.parse_format);
        self.parse_status.append_value(&row.parse_status);
        append_opt_dict(&mut self.thread_name, row.thread_name.as_deref());
        append_opt_dict(&mut self.process_name, row.process_name.as_deref());
        append_opt_i64(&mut self.process_pid, row.process_pid);
        self.body.append_value(&row.body);
        self.normalized_template.append_value(&row.normalized_template);
        self.error_kind.append_value(&row.error_kind);
        append_opt_dict(&mut self.exception_type, row.exception_type.as_deref());
        self.template_id.append_value(&row.template_id);
        self.raw_hash.append_value(&row.raw_hash);
        self.classification.append_value(&row.classification);
        append_opt_dict(&mut self.classification_reason, row.classification_reason.as_deref());
        self.service_name.append_value(&row.service_name);
        append_opt_dict(&mut self.service_instance_id, row.service_instance_id.as_deref());
        append_opt_dict(&mut self.worker_id, row.worker_id.as_deref());
        append_opt_dict(&mut self.server_kind, row.server_kind.as_deref());
        append_opt_dict(&mut self.trace_id, row.trace_id.as_deref());
        append_opt_dict(&mut self.span_id, row.span_id.as_deref());
        append_opt_dict(&mut self.container_id, row.container_id.as_deref());
        append_opt_dict(&mut self.k8s_pod_name, row.k8s_pod_name.as_deref());
        append_opt_dict(&mut self.k8s_container_name, row.k8s_container_name.as_deref());
        self.attributes_json.append_value(&row.attributes_json);
        self.resource_json.append_value(&row.resource_json);
        append_opt_dict(&mut self.raw, row.raw.as_deref());
        self.len += 1;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn finish(mut self) -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.event_id.finish()),
            Arc::new(self.run_id.finish()),
            Arc::new(self.source.finish()),
            Arc::new(self.offset.finish()),
            Arc::new(self.line_count.finish()),
            Arc::new(self.timestamp.finish()),
            Arc::new(self.observed_timestamp.finish()),
            Arc::new(self.severity_text.finish()),
            Arc::new(self.severity_number.finish()),
            Arc::new(self.parse_format.finish()),
            Arc::new(self.parse_status.finish()),
            Arc::new(self.thread_name.finish()),
            Arc::new(self.process_name.finish()),
            Arc::new(self.process_pid.finish()),
            Arc::new(self.body.finish()),
            Arc::new(self.normalized_template.finish()),
            Arc::new(self.error_kind.finish()),
            Arc::new(self.exception_type.finish()),
            Arc::new(self.template_id.finish()),
            Arc::new(self.raw_hash.finish()),
            Arc::new(self.classification.finish()),
            Arc::new(self.classification_reason.finish()),
            Arc::new(self.service_name.finish()),
            Arc::new(self.service_instance_id.finish()),
            Arc::new(self.worker_id.finish()),
            Arc::new(self.server_kind.finish()),
            Arc::new(self.trace_id.finish()),
            Arc::new(self.span_id.finish()),
            Arc::new(self.container_id.finish()),
            Arc::new(self.k8s_pod_name.finish()),
            Arc::new(self.k8s_container_name.finish()),
            Arc::new(self.attributes_json.finish()),
            Arc::new(self.resource_json.finish()),
            Arc::new(self.raw.finish()),
        ];
        RecordBatch::try_new(events_schema(), arrays).expect("events batch")
    }
}

#[inline]
fn append_opt_dict(builder: &mut DictBuilder, value: Option<&str>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}
