//! End-to-end Drain phase orchestrator.
//!
//! Replaces the previous Python `_apply_drain_to_templates` helper. The
//! events Arrow IPC file is rewritten entirely Rust-side with the
//! template_id dictionary remapped to canonical ids; Python never touches
//! per-event data. Templates cross the FFI boundary in-memory because
//! Python owns rich metadata (classification, issue_kind, etc.) that lives
//! outside the templates IPC schema — but that's O(hundreds), not events.
//!
//! Events table dictionary remap only touches the *dictionary values*
//! (cardinality = unique templates ≈ thousands); per-event int32 indices
//! are streamed through untouched.
//!
//! Layout invariants we rely on:
//!   - `template_id` in events.arrow is `Dictionary<Int32, Utf8>`

use crate::drain::{build_template_remap, CanonicalRow, Drainer, TemplateInput};
use arrow::array::{Array, ArrayRef, DictionaryArray, RecordBatch, StringArray, StringBuilder};
use arrow::datatypes::{Int32Type, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
pub struct DrainPhaseSummary {
    pub elapsed_ms: u64,
    pub input_templates: usize,
    pub output_clusters: usize,
    pub merged_template_groups: usize,
    pub events_total: usize,
    pub drain_clusters_total: usize,
    pub drain_state_path: String,
    pub events_rewritten: bool,
}

pub struct DrainPhaseResult {
    pub summary: DrainPhaseSummary,
    pub remap: HashMap<String, String>,
    pub canonical: Vec<CanonicalRow>,
}

/// Run the Drain phase end-to-end: mine over `templates`, persist tree
/// state atomically, rewrite events.arrow with the remapped dictionary.
///
/// Templates flow through the FFI boundary as an in-memory `Vec` because
/// the Python caller owns rich metadata (classification, issue_kind, …)
/// that lives outside the templates IPC schema. Python applies the
/// returned `remap` to its own templates list to preserve that metadata.
///
/// Events stay entirely in Rust: read from `events_ipc`, dictionary
/// remapped streaming, written to `events_out`. If `events_out` is `None`
/// the events.arrow is left untouched (caller already wrote it elsewhere
/// or doesn't need the rewrite).
pub fn apply_drain_phase(
    events_ipc: Option<&Path>,
    events_out: Option<&Path>,
    drain_state: &Path,
    templates: Vec<TemplateInput>,
    depth: usize,
    sim_th: f64,
    max_children: usize,
) -> Result<DrainPhaseResult, String> {
    let started = Instant::now();
    let input_templates_n = templates.len();

    // Trivial short-circuit. We still pass through events.arrow if the
    // caller asked, so the output path exists with valid Arrow IPC.
    if templates.len() <= 1 {
        let events_total = match (events_ipc, events_out) {
            (Some(src), Some(dst)) => copy_arrow_stream(src, dst)?,
            _ => 0,
        };
        return Ok(DrainPhaseResult {
            summary: DrainPhaseSummary {
                elapsed_ms: started.elapsed().as_millis() as u64,
                input_templates: input_templates_n,
                output_clusters: input_templates_n,
                merged_template_groups: 0,
                events_total,
                drain_clusters_total: 0,
                drain_state_path: drain_state.to_string_lossy().into_owned(),
                events_rewritten: false,
            },
            remap: HashMap::new(),
            canonical: Vec::new(),
        });
    }

    // ---- 1. Load / init Drain state -----------------------------------
    let mut drainer = if drain_state.exists() {
        match std::fs::read_to_string(drain_state) {
            Ok(payload) => Drainer::from_json(&payload).unwrap_or_else(|_| {
                // Corrupt state (mid-write crash, manual edit) — start fresh.
                Drainer::new(depth, sim_th, max_children)
            }),
            Err(_) => Drainer::new(depth, sim_th, max_children),
        }
    } else {
        Drainer::new(depth, sim_th, max_children)
    };

    // ---- 2. Run Drain --------------------------------------------------
    let (remap, canonical_rows) = build_template_remap(&mut drainer, &templates);

    // ---- 3. Persist Drain state atomically ----------------------------
    if let Some(parent) = drain_state.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
    }
    let drain_json = drainer.to_json().map_err(|e| e.to_string())?;
    let drain_state_tmp = drain_state.with_extension(format!(
        "{}.tmp",
        drain_state.extension().and_then(|s| s.to_str()).unwrap_or("json")
    ));
    std::fs::write(&drain_state_tmp, drain_json).map_err(|e| e.to_string())?;
    std::fs::rename(&drain_state_tmp, drain_state).map_err(|e| e.to_string())?;

    let drain_clusters_total = drainer.next_cluster_id.saturating_sub(1) as usize;
    let merged_groups = canonical_rows
        .iter()
        .filter(|r| r.member_template_ids.len() > 1)
        .count();

    // ---- 4. Rewrite events.arrow if requested -------------------------
    let actually_remaps = remap.iter().any(|(old, new)| old != new);
    let (events_total, events_rewritten) = match (events_ipc, events_out) {
        (Some(src), Some(dst)) if actually_remaps => {
            (rewrite_events_with_remap(src, dst, &remap)?, true)
        }
        (Some(src), Some(dst)) => (copy_arrow_stream(src, dst)?, false),
        _ => (0, false),
    };

    Ok(DrainPhaseResult {
        summary: DrainPhaseSummary {
            elapsed_ms: started.elapsed().as_millis() as u64,
            input_templates: input_templates_n,
            output_clusters: canonical_rows.len(),
            merged_template_groups: merged_groups,
            events_total,
            drain_clusters_total,
            drain_state_path: drain_state.to_string_lossy().into_owned(),
            events_rewritten,
        },
        remap,
        canonical: canonical_rows,
    })
}

// =====================================================================
// Events IPC streaming rewrite
// =====================================================================

fn rewrite_events_with_remap(
    events_ipc: &Path,
    events_out: &Path,
    remap: &HashMap<String, String>,
) -> Result<usize, String> {
    if let Some(parent) = events_out.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
    }

    let in_file = File::open(events_ipc).map_err(|e| format!("open events ipc: {e}"))?;
    let reader = StreamReader::try_new(BufReader::new(in_file), None)
        .map_err(|e| format!("create events reader: {e}"))?;
    let in_schema = Arc::new(reader.schema().as_ref().clone());

    let out_file = File::create(events_out).map_err(|e| format!("create events out: {e}"))?;
    let mut writer = StreamWriter::try_new(
        BufWriter::with_capacity(1 << 20, out_file),
        in_schema.as_ref(),
    )
    .map_err(|e| format!("create events writer: {e}"))?;

    let tid_idx = in_schema
        .fields()
        .iter()
        .position(|f| f.name() == "template_id")
        .ok_or_else(|| "events.arrow has no template_id column".to_string())?;

    let mut total_events: usize = 0;
    for batch in reader {
        let batch = batch.map_err(|e| format!("read events batch: {e}"))?;
        let remapped = remap_batch_template_id(&batch, tid_idx, remap, &in_schema)?;
        total_events += remapped.num_rows();
        writer.write(&remapped).map_err(|e| format!("write events batch: {e}"))?;
    }
    writer.finish().map_err(|e| format!("finish events writer: {e}"))?;
    Ok(total_events)
}

fn remap_batch_template_id(
    batch: &RecordBatch,
    tid_idx: usize,
    remap: &HashMap<String, String>,
    schema: &Arc<Schema>,
) -> Result<RecordBatch, String> {
    let col = batch.column(tid_idx);

    // The events schema declares template_id as Dictionary<Int32, Utf8> —
    // but downcast defensively in case the producer ever changes the
    // encoding. Plain Utf8 path is the slow fallback.
    let new_col: ArrayRef = if let Some(dict) =
        col.as_any().downcast_ref::<DictionaryArray<Int32Type>>()
    {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "template_id dictionary values not StringArray".to_string())?;
        // Build a remapped values array. The keys / indices stay byte-identical.
        let mut new_values = StringBuilder::with_capacity(values.len(), values.len() * 32);
        for i in 0..values.len() {
            if values.is_null(i) {
                new_values.append_null();
            } else {
                let v = values.value(i);
                let mapped = remap.get(v).map(String::as_str).unwrap_or(v);
                new_values.append_value(mapped);
            }
        }
        let new_values_arr: ArrayRef = Arc::new(new_values.finish());
        Arc::new(DictionaryArray::<Int32Type>::try_new(
            dict.keys().clone(),
            new_values_arr,
        )
        .map_err(|e| e.to_string())?)
    } else if let Some(plain) = col.as_any().downcast_ref::<StringArray>() {
        let mut builder = StringBuilder::with_capacity(plain.len(), plain.len() * 32);
        for i in 0..plain.len() {
            if plain.is_null(i) {
                builder.append_null();
            } else {
                let v = plain.value(i);
                let mapped = remap.get(v).map(String::as_str).unwrap_or(v);
                builder.append_value(mapped);
            }
        }
        Arc::new(builder.finish())
    } else {
        return Err(format!(
            "template_id column has unexpected type {:?}",
            col.data_type()
        ));
    };

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[tid_idx] = new_col;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| e.to_string())
}

/// Verbatim copy of an Arrow IPC stream from `src` to `dst`. Returns the
/// total row count across all batches (so the summary still has it).
fn copy_arrow_stream(src: &Path, dst: &Path) -> Result<usize, String> {
    if let Some(parent) = dst.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
    }
    let in_file = File::open(src).map_err(|e| format!("open src ipc: {e}"))?;
    let reader = StreamReader::try_new(BufReader::new(in_file), None)
        .map_err(|e| format!("create reader: {e}"))?;
    let schema = Arc::new(reader.schema().as_ref().clone());
    let out_file = File::create(dst).map_err(|e| format!("create dst ipc: {e}"))?;
    let mut writer = StreamWriter::try_new(
        BufWriter::with_capacity(1 << 20, out_file),
        schema.as_ref(),
    )
    .map_err(|e| format!("create writer: {e}"))?;
    let mut total: usize = 0;
    for batch in reader {
        let batch = batch.map_err(|e| format!("read batch: {e}"))?;
        total += batch.num_rows();
        writer.write(&batch).map_err(|e| format!("write batch: {e}"))?;
    }
    writer.finish().map_err(|e| format!("finish writer: {e}"))?;
    Ok(total)
}

