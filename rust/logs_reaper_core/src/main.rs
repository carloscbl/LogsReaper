use logs_reaper_core::{
    aggregate_templates, columnar, is_continuation_bytes, parse_logical_record, reset_dedup_cache,
    RustEventRow, RustTemplateRow,
};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use memchr::memchr_iter;
use memmap2::Mmap;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Debug, Serialize)]
struct EngineSummary {
    engine: String,
    service_name: String,
    input_file: String,
    event_count: usize,
    template_count: usize,
    input_bytes: u64,
    input_gigabytes: f64,
    scan_duration_seconds: f64,
    throughput_gb_per_second: f64,
    events_per_second: f64,
    hash_algorithm: String,
    templates: Vec<RustTemplateRow>,
}

#[derive(Debug, Serialize)]
struct ScanPayload {
    engine: String,
    service_name: String,
    input_file: String,
    event_count: usize,
    template_count: usize,
    input_bytes: u64,
    input_gigabytes: f64,
    scan_duration_seconds: f64,
    throughput_gb_per_second: f64,
    events_per_second: f64,
    hash_algorithm: String,
    events: Vec<RustEventRow>,
    templates: Vec<RustTemplateRow>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let total_bytes = std::fs::metadata(&args.input)?.len();
    let start_offset = args.start_offset.min(total_bytes);
    let input_bytes = total_bytes - start_offset;
    let started = Instant::now();
    reset_dedup_cache();
    let source = args.input.to_string_lossy().to_string();

    // Stream-parse logical records straight off the mmap, avoiding the intermediate Vec of
    // owned `String`s the previous implementation kept around.
    let parsed = parse_mmap(
        &args.input,
        start_offset,
        &args.service,
        &args.run_id,
        &source,
        &args.observed_timestamp,
        args.include_raw,
        args.progress,
        input_bytes,
    )?;
    let templates = aggregate_templates(&parsed);
    let duration = started.elapsed().as_secs_f64().max(1e-9);
    let input_gigabytes = input_bytes as f64 / (1024_f64 * 1024_f64 * 1024_f64);
    let summary_meta = SummaryMeta {
        engine: "rust".to_string(),
        service_name: args.service.clone(),
        input_file: source.clone(),
        event_count: parsed.len(),
        template_count: templates.len(),
        input_bytes,
        input_gigabytes,
        scan_duration_seconds: duration,
        throughput_gb_per_second: input_gigabytes / duration,
        events_per_second: parsed.len() as f64 / duration,
        hash_algorithm: "xxh3-128 + blake3-128".to_string(),
    };

    let columnar_requested = args.out_events.is_some() || args.out_templates.is_some();
    if columnar_requested {
        let events_batch = columnar::events_to_batch(&parsed);
        let templates_batch = columnar::templates_to_batch(&templates);
        if let Some(path) = &args.out_events {
            write_columnar(path, &events_batch, args.out_format.as_str())?;
        }
        if let Some(path) = &args.out_templates {
            write_columnar(path, &templates_batch, args.out_format.as_str())?;
        }
        // When IPC/Parquet is in play, stdout carries only the small summary metadata.
        println!("{}", serde_json::to_string(&summary_meta)?);
        return Ok(());
    }

    match args.mode.as_str() {
        "scan" => {
            let payload = ScanPayload {
                engine: summary_meta.engine.clone(),
                service_name: summary_meta.service_name.clone(),
                input_file: summary_meta.input_file.clone(),
                event_count: summary_meta.event_count,
                template_count: summary_meta.template_count,
                input_bytes: summary_meta.input_bytes,
                input_gigabytes: summary_meta.input_gigabytes,
                scan_duration_seconds: summary_meta.scan_duration_seconds,
                throughput_gb_per_second: summary_meta.throughput_gb_per_second,
                events_per_second: summary_meta.events_per_second,
                hash_algorithm: summary_meta.hash_algorithm.clone(),
                events: parsed,
                templates,
            };
            println!("{}", serde_json::to_string(&payload)?);
        }
        _ => {
            let summary = EngineSummary {
                engine: summary_meta.engine,
                service_name: summary_meta.service_name,
                input_file: summary_meta.input_file,
                event_count: summary_meta.event_count,
                template_count: summary_meta.template_count,
                input_bytes: summary_meta.input_bytes,
                input_gigabytes: summary_meta.input_gigabytes,
                scan_duration_seconds: summary_meta.scan_duration_seconds,
                throughput_gb_per_second: summary_meta.throughput_gb_per_second,
                events_per_second: summary_meta.events_per_second,
                hash_algorithm: summary_meta.hash_algorithm,
                templates,
            };
            println!("{}", serde_json::to_string(&summary)?);
        }
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct SummaryMeta {
    engine: String,
    service_name: String,
    input_file: String,
    event_count: usize,
    template_count: usize,
    input_bytes: u64,
    input_gigabytes: f64,
    scan_duration_seconds: f64,
    throughput_gb_per_second: f64,
    events_per_second: f64,
    hash_algorithm: String,
}

fn write_columnar(path: &PathBuf, batch: &RecordBatch, format: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(path)?;
    let writer = BufWriter::with_capacity(1 << 20, file);
    match format {
        "parquet" => {
            // Phase B / item 11: write Parquet directly from Rust. Skips the Python encode pass.
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .set_dictionary_enabled(true)
                .build();
            let mut pq = ArrowWriter::try_new(writer, batch.schema(), Some(props))?;
            pq.write(batch)?;
            pq.close()?;
        }
        _ => {
            // Phase B / item 9: Arrow IPC stream is the new boundary contract between Rust and
            // the Python management layer. Python reads it zero-copy via pyarrow.ipc.open_stream.
            let mut sw = StreamWriter::try_new(writer, batch.schema().as_ref())?;
            sw.write(batch)?;
            sw.finish()?;
        }
    }
    Ok(())
}

#[derive(Debug)]
struct Args {
    input: PathBuf,
    service: String,
    run_id: String,
    observed_timestamp: String,
    include_raw: bool,
    mode: String,
    start_offset: u64,
    progress: bool,
    out_events: Option<PathBuf>,
    out_templates: Option<PathBuf>,
    out_format: String,
}

fn parse_args() -> Result<Args, String> {
    let mut input: Option<PathBuf> = None;
    let mut service: Option<String> = None;
    let mut run_id: String = "RUST_RUN".to_string();
    let mut observed_timestamp: String = "1970-01-01T00:00:00Z".to_string();
    let mut include_raw = false;
    let mut mode = "summary".to_string();
    let mut start_offset: u64 = 0;
    let mut progress = false;
    let mut out_events: Option<PathBuf> = None;
    let mut out_templates: Option<PathBuf> = None;
    let mut out_format: String = "ipc".to_string();
    let mut iter = env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--input" => input = iter.next().map(PathBuf::from),
            "--service" => service = iter.next(),
            "--run-id" => run_id = iter.next().ok_or_else(|| "--run-id requires a value".to_string())?,
            "--observed-timestamp" => {
                observed_timestamp = iter
                    .next()
                    .ok_or_else(|| "--observed-timestamp requires a value".to_string())?
            }
            "--mode" => mode = iter.next().ok_or_else(|| "--mode requires a value".to_string())?,
            "--include-raw" => include_raw = true,
            "--start-offset" => {
                start_offset = iter
                    .next()
                    .ok_or_else(|| "--start-offset requires a value".to_string())?
                    .parse::<u64>()
                    .map_err(|err| format!("--start-offset must be a non-negative integer: {err}"))?;
            }
            "--progress" => progress = true,
            "--out-events" => {
                out_events = Some(PathBuf::from(
                    iter.next().ok_or_else(|| "--out-events requires a path".to_string())?,
                ));
            }
            "--out-templates" => {
                out_templates = Some(PathBuf::from(
                    iter.next().ok_or_else(|| "--out-templates requires a path".to_string())?,
                ));
            }
            "--out-format" => {
                out_format = iter
                    .next()
                    .ok_or_else(|| "--out-format requires a value (ipc|parquet)".to_string())?;
                if out_format != "ipc" && out_format != "parquet" {
                    return Err(format!("invalid --out-format '{out_format}', expected 'ipc' or 'parquet'"));
                }
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(Args {
        input: input.ok_or_else(|| "--input is required".to_string())?,
        service: service.ok_or_else(|| "--service is required".to_string())?,
        run_id,
        observed_timestamp,
        include_raw,
        mode,
        start_offset,
        progress,
        out_events,
        out_templates,
        out_format,
    })
}

#[allow(clippy::too_many_arguments)]
fn parse_mmap(
    path: &PathBuf,
    start_offset: u64,
    service: &str,
    run_id: &str,
    source: &str,
    observed_timestamp: &str,
    include_raw: bool,
    progress: bool,
    progress_total: u64,
) -> Result<Vec<RustEventRow>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    // SAFETY: we map the file read-only; the OS handles paging. We never write through this view.
    let mmap = unsafe { Mmap::map(&file)? };
    let bytes: &[u8] = &mmap;
    let start = (start_offset as usize).min(bytes.len());
    let region = &bytes[start..];

    let mut parsed: Vec<RustEventRow> = Vec::with_capacity(region.len() / 256);
    let mut current_start: Option<usize> = None;
    let mut current_lines: usize = 0;
    let mut line_start: usize = 0;
    let mut last_progress_tick = Instant::now();
    let mut last_progress_bytes: u64 = 0;
    let progress_interval = Duration::from_millis(80);
    let progress_byte_interval: u64 = 256 * 1024;
    if progress {
        emit_progress(0, progress_total, parsed.len());
    }

    // memchr is SIMD-accelerated on x86 (SSE2/AVX2) and arm64.
    for newline_idx in memchr_iter(b'\n', region) {
        let line = &region[line_start..newline_idx];
        // Trailing '\r' on CRLF inputs is treated as part of the logical line for cleanliness.
        let line = strip_trailing_cr(line);
        let line_offset_abs = (start + line_start) as u64;
        let is_continuation = is_continuation_bytes(line) && current_start.is_some();
        if is_continuation {
            current_lines += 1;
        } else {
            if let Some(rec_start) = current_start.take() {
                emit_record(
                    region,
                    rec_start,
                    line_start,
                    start,
                    current_lines,
                    service,
                    run_id,
                    source,
                    observed_timestamp,
                    include_raw,
                    &mut parsed,
                );
            }
            current_start = Some(line_start);
            current_lines = 1;
            let _ = line_offset_abs; // reserved for future use; kept to keep math explicit
        }
        line_start = newline_idx + 1;

        if progress {
            let bytes_done = line_start as u64;
            if last_progress_tick.elapsed() >= progress_interval
                || bytes_done.saturating_sub(last_progress_bytes) >= progress_byte_interval
            {
                emit_progress(bytes_done, progress_total, parsed.len());
                last_progress_tick = Instant::now();
                last_progress_bytes = bytes_done;
            }
        }
    }
    // Trailing chunk after the last '\n' (or the entire file if it has no newline).
    if line_start < region.len() {
        let tail = strip_trailing_cr(&region[line_start..]);
        let is_continuation = is_continuation_bytes(tail) && current_start.is_some();
        if is_continuation {
            current_lines += 1;
        } else {
            if let Some(rec_start) = current_start.take() {
                emit_record(
                    region,
                    rec_start,
                    line_start,
                    start,
                    current_lines,
                    service,
                    run_id,
                    source,
                    observed_timestamp,
                    include_raw,
                    &mut parsed,
                );
            }
            current_start = Some(line_start);
            current_lines = 1;
        }
        if let Some(rec_start) = current_start.take() {
            emit_record(
                region,
                rec_start,
                region.len(),
                start,
                current_lines,
                service,
                run_id,
                source,
                observed_timestamp,
                include_raw,
                &mut parsed,
            );
        }
    } else if let Some(rec_start) = current_start.take() {
        emit_record(
            region,
            rec_start,
            line_start,
            start,
            current_lines,
            service,
            run_id,
            source,
            observed_timestamp,
            include_raw,
            &mut parsed,
        );
    }
    if progress {
        emit_progress(progress_total, progress_total, parsed.len());
    }
    Ok(parsed)
}

#[allow(clippy::too_many_arguments)]
fn emit_record(
    region: &[u8],
    rec_start: usize,
    rec_end: usize,
    absolute_origin: usize,
    line_count: usize,
    service: &str,
    run_id: &str,
    source: &str,
    observed_timestamp: &str,
    include_raw: bool,
    out: &mut Vec<RustEventRow>,
) {
    let raw_bytes = strip_trailing_newline(&region[rec_start..rec_end]);
    // The hot path operates on UTF-8 lines; we lossily convert to keep it robust against
    // occasional malformed bytes the way the previous lossy `String::from_utf8` did.
    let raw_str = match std::str::from_utf8(raw_bytes) {
        Ok(s) => std::borrow::Cow::Borrowed(s),
        Err(_) => String::from_utf8_lossy(raw_bytes),
    };
    let offset = (absolute_origin + rec_start) as u64;
    let row = parse_logical_record(
        &raw_str,
        service,
        run_id,
        source,
        offset,
        line_count,
        observed_timestamp,
        include_raw,
    );
    out.push(row);
}

#[inline]
fn strip_trailing_cr(line: &[u8]) -> &[u8] {
    if let Some((&last, head)) = line.split_last() {
        if last == b'\r' {
            return head;
        }
    }
    line
}

#[inline]
fn strip_trailing_newline(record: &[u8]) -> &[u8] {
    let mut end = record.len();
    while end > 0 {
        let b = record[end - 1];
        if b == b'\n' || b == b'\r' {
            end -= 1;
        } else {
            break;
        }
    }
    &record[..end]
}

fn emit_progress(bytes_read: u64, bytes_total: u64, events: usize) {
    let stderr = std::io::stderr();
    let mut handle = stderr.lock();
    let _ = writeln!(
        handle,
        "PROGRESS bytes_read={bytes_read} bytes_total={bytes_total} events={events}"
    );
}
