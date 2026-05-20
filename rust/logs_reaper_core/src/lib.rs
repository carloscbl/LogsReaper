use once_cell::sync::Lazy;
use regex::{Captures, Regex};
use serde::Serialize;
use serde_json::{Map, Value};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value as SonicValue};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;

// FxHashMap (rustc-hash) is 2-3× faster than the default SipHash HashMap for
// the kind of keys we feed it (u128 line fingerprints, String template ids).
// Template aggregation runs once per event so the win compounds. We alias
// rather than search-and-replace to keep std::collections::HashMap available
// for places that need ordering/security guarantees.
type FxHashMap<K, V> = HashMap<K, V, rustc_hash::FxBuildHasher>;
#[cfg(feature = "python")]
use rayon::prelude::*;
use twox_hash::xxh3::{Hash128, HasherExt};

pub mod columnar;
pub mod drain;
pub mod drain_phase;

// Cross-run stable identity uses xxh3-128 (same family as `event_hash` /
// `raw_hash`). The previous implementation used BLAKE3 motivated by
// "cryptographic strength, low collision" — but template ids are not
// adversarial inputs, just dedup keys. xxh3-128 is ~10× faster than BLAKE3
// (~30 GiB/s vs ~3 GiB/s on modern x86) and the collision probability at our
// scale (≤10^5 distinct templates per service) is ~10^-30 with 128 bits.
// Format remains a 32-char lowercase-hex string so existing baselines /
// dashboards consuming the field don't need to change.
pub fn template_hash(service: &str, severity: &str, normalized_template: &str, error_kind: &str) -> String {
    let mut hasher = Hash128::with_seed(0);
    hasher.write(service.as_bytes());
    hasher.write(&[0x1f]);
    hasher.write(severity.as_bytes());
    hasher.write(&[0x1f]);
    hasher.write(normalized_template.as_bytes());
    hasher.write(&[0x1f]);
    hasher.write(error_kind.as_bytes());
    format!("{:032x}", xxh3_finish_u128(&hasher))
}

// Internal-only identity uses xxh3-128 (3-4x faster, non-cryptographic, ok for dedup keys).
pub fn event_hash(run_id: &str, source: &str, offset: u64, raw_hash: &str) -> String {
    let mut hasher = Hash128::with_seed(0);
    hasher.write(run_id.as_bytes());
    hasher.write(&[0x1f]);
    hasher.write(source.as_bytes());
    hasher.write(&[0x1f]);
    hasher.write(&offset.to_le_bytes());
    hasher.write(&[0x1f]);
    hasher.write(raw_hash.as_bytes());
    format!("{:032x}", xxh3_finish_u128(&hasher))
}

pub fn stable_hash(parts: &[&str]) -> String {
    let payload = parts.join("\u{1f}");
    blake3::hash(payload.as_bytes()).to_hex().chars().take(32).collect()
}

pub fn raw_hash(raw: &str) -> String {
    raw_hash_bytes(raw.as_bytes())
}

pub fn raw_hash_bytes(bytes: &[u8]) -> String {
    let mut hasher = Hash128::with_seed(0);
    hasher.write(bytes);
    format!("{:032x}", xxh3_finish_u128(&hasher))
}

#[inline]
fn xxh3_finish_u128(hasher: &Hash128) -> u128 {
    hasher.finish_ext()
}

/// Fingerprint estable para `template_id`. Toma la primera línea no vacía
/// del template **ya normalizado** y la corta en:
///   1. el primer marcador de "contenido variable" (payload/body/data/...)
///   2. o 200 chars máx si no encuentra marcador.
/// Esto colapsa el mismo error capturado con payloads JSON distintos o con
/// tracebacks de profundidad variable — pasaba con `ValueError: Either provide
/// a trace id...` cuyo payload de `ProvisionUpdated` cambia entre cuentas, y
/// con `DuplicateKeyError` cuyo traceback se trunca en sitios distintos.
///
/// La versión anterior recibía el body crudo y llamaba a `normalize_message`
/// internamente sobre la primera línea — duplicaba trabajo regex con el
/// `normalize_message(body)` que ya hace `build_normalized`. Ahora aceptamos
/// directamente el template normalizado y sólo extraemos la primera línea.
pub fn headline_fingerprint_from_normalized(normalized_body: &str) -> String {
    let headline = normalized_body
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("");
    headline_fingerprint_cut(headline)
}

#[inline]
fn headline_fingerprint_cut(headline: &str) -> String {
    let cut_at = HEADLINE_CUT_RE
        .find(headline)
        .map(|m| m.start())
        .unwrap_or(headline.len());
    let prefix: String = headline[..cut_at].chars().take(200).collect();
    prefix.trim_end_matches(|c: char| c == ',' || c == ':' || c == ' ' || c == '-').to_string()
}

/// Back-compat para los call-sites que aún pasan el body crudo (tests + el
/// binding pyo3 público). Internamente delegamos en la versión optimizada
/// tras normalizar una sola vez. La hot path en `build_normalized` ya no
/// pasa por aquí: usa `headline_fingerprint_from_normalized` directamente.
pub fn headline_fingerprint(body: &str) -> String {
    let headline = body
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or("");
    let normalized = normalize_message(headline);
    headline_fingerprint_cut(&normalized)
}

pub fn normalize_message(value: &str) -> String {
    let stripped: Cow<'_, str> = strip_ansi_cow(value);
    let mut text: Cow<'_, str> = stripped;
    // Each replace_all returns Cow::Borrowed if no match — we keep it borrowed and only allocate
    // when an actual substitution happens. The byte-level preconditions below are cheap shortcuts
    // before invoking the regex engine.
    if text.contains('@') {
        text = cow_replace_all(text, &EMAIL_RE, "<EMAIL>");
    }
    if text.contains("://") {
        text = cow_replace_all(text, &URL_RE, "<URL>");
    }
    if text.contains('-') {
        // UUID + ISO timestamp + old slash timestamp share the same byte trigger.
        text = cow_replace_all(text, &UUID_RE, "<UUID>");
        text = cow_replace_all(text, &ISO_TS_RE, "<TIMESTAMP>");
        text = cow_replace_all(text, &OLD_TS_RE, "<TIMESTAMP>");
    }
    if text.contains('.') {
        text = cow_replace_all(text, &IPV4_RE, "<IP>");
    }
    if text.contains(':') {
        text = cow_replace_all(text, &IPV6_RE, "<IP>");
        text = cow_replace_with(text, &PORT_RE, |caps: &Captures| {
            let suffix = caps.name("suffix").map(|m| m.as_str()).unwrap_or("");
            format!("{}<PORT>{suffix}", &caps["prefix"])
        });
    }
    if text.contains('/') {
        text = cow_replace_all(text, &PATH_RE, "<PATH>");
    }
    if text.contains("0x") || text.contains("0X") {
        text = cow_replace_all(text, &HEX_RE, "<HEX>");
    }
    if text.as_bytes().iter().any(|byte| byte.is_ascii_digit()) {
        // `regex` crate has no lookarounds, so the hex-blob regexes capture the surrounding
        // non-hex byte (or BOS/EOS) explicitly via named groups and the closure re-emits them.
        // Using only `\b` would miss `prefix_<hex>` tokens because `_` is a word char.
        text = cow_replace_with(text, &TRACE_ID_RE, |caps: &Captures| replace_hex_blob(caps, "<TRACE_ID>"));
        text = cow_replace_with(text, &OBJECT_ID_RE, |caps: &Captures| replace_hex_blob(caps, "<OBJECT_ID>"));
        text = cow_replace_with(text, &SPAN_ID_RE, |caps: &Captures| replace_hex_blob(caps, "<SPAN_ID>"));
        text = Cow::Owned(replace_numbers(&text));
        // Phase-1 generic ID masking: after the dictionary regex passes, walk
        // the remaining tokens and collapse opaque app-defined IDs that none of
        // the named patterns above caught. Examples this rescues:
        //   conv_50a1442a002b   (12-hex w/ word prefix; OBJECT_ID needs 24)
        //   dev_a3f9b2c        (alnum suffix, mixed case+digits)
        //   aBc123Def456GhI    (bare base62-ish opaque ID, no prefix)
        // The two heuristics are conservative on purpose to avoid masking
        // benign tokens like `nsclient_1`, `mongo_v2`, `error500`.
        text = Cow::Owned(entropy_mask(&text));
    }
    let trimmed = text.trim();
    cow_replace_all(Cow::Borrowed(trimmed), &SPACE_RE, " ").into_owned()
}

/// Walk `value` byte-by-byte, find token boundaries (`[A-Za-z0-9_-]+`), and
/// mask the body of tokens that look like opaque identifiers while preserving
/// the rest verbatim. Conservative: only fires on tokens that are *unambiguous*
/// opaque IDs. Service names like `nsclient_1`, version tags like `kafka_v2`,
/// and short alphanumeric mixes stay as-is so they remain readable in templates.
///
/// Two firing rules (in priority order):
///   1. `<alpha_prefix>_<hex8..40>`  →  `<alpha_prefix>_<HEX>`
///      Keeps the semantic prefix so two events `conv_aaaa` and `conv_bbbb`
///      collapse to the same `conv_<HEX>` token but a `req_aaaa` event stays
///      distinguishable from `conv_aaaa`.
///   2. `<alnum 16..>` with both letters AND digits  →  `<ID>`
///      Catches bare opaque tokens (base62/base64) with no prefix.
fn entropy_mask(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if !is_id_token_char(b) {
            // Non-token byte (whitespace, punctuation, multibyte continuation)
            // — emit verbatim. For multibyte UTF-8 continuation bytes the slice
            // here is a single byte but the original UTF-8 sequence will be
            // reconstructed correctly because all subsequent bytes also fail
            // is_id_token_char and get pushed individually.
            out.push(b as char);
            i += 1;
            continue;
        }
        let start = i;
        while i < bytes.len() && is_id_token_char(bytes[i]) {
            i += 1;
        }
        let token = &input[start..i];
        if let Some(masked) = mask_opaque_token(token) {
            out.push_str(&masked);
        } else {
            out.push_str(token);
        }
    }
    out
}

#[inline]
fn is_id_token_char(b: u8) -> bool {
    matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-')
}

/// Returns `Some(masked)` if the token is an opaque ID; `None` to keep verbatim.
fn mask_opaque_token(token: &str) -> Option<String> {
    if token.len() < 6 {
        return None;
    }
    let bytes = token.as_bytes();
    // Rule 1: prefix_hex. Require ASCII-alpha prefix of len 2..=15, a single
    // underscore separator, and 6..=40 lowercase-hex digits. The hex digit
    // requirement is the discriminator vs. benign tokens like `nsclient_1`.
    if let Some(underscore_pos) = bytes.iter().position(|&b| b == b'_') {
        let prefix = &bytes[..underscore_pos];
        let suffix = &bytes[underscore_pos + 1..];
        // `all(is_ascii_hexdigit)` alone is enough to disambiguate IDs from
        // word_word pairs: any non-hex letter (g..z) in the suffix rejects.
        // The 6-char minimum protects 3-letter words that happen to be hex
        // (`ace`, `bad`, `dad`...) from being masked.
        if (2..=15).contains(&prefix.len())
            && prefix.iter().all(|b| b.is_ascii_alphabetic())
            && (6..=40).contains(&suffix.len())
            && suffix.iter().all(|b| b.is_ascii_hexdigit())
        {
            let mut out = String::with_capacity(prefix.len() + 7);
            out.push_str(&token[..underscore_pos + 1]);
            out.push_str("<HEX>");
            return Some(out);
        }
    }
    // Rule 2: bare opaque ID. Length >= 16, contains both letters AND digits,
    // ASCII-alnum only (no underscore/dash). Length threshold is chosen to
    // avoid masking common alpha+digit tokens like `kafka9092`, `python3` or
    // version strings like `v1beta1` (longest sees ~10 chars in practice).
    if token.len() >= 16
        && bytes.iter().all(|b| b.is_ascii_alphanumeric())
        && bytes.iter().any(|b| b.is_ascii_alphabetic())
        && bytes.iter().any(|b| b.is_ascii_digit())
    {
        return Some("<ID>".to_string());
    }
    None
}

#[inline]
fn cow_replace_all<'a>(text: Cow<'a, str>, re: &Regex, replacement: &str) -> Cow<'a, str> {
    match text {
        Cow::Borrowed(borrowed) => re.replace_all(borrowed, replacement),
        Cow::Owned(owned) => Cow::Owned(re.replace_all(&owned, replacement).into_owned()),
    }
}

#[inline]
fn cow_replace_with<'a, F>(text: Cow<'a, str>, re: &Regex, mut f: F) -> Cow<'a, str>
where
    F: FnMut(&Captures) -> String,
{
    match text {
        Cow::Borrowed(borrowed) => re.replace_all(borrowed, |caps: &Captures| f(caps)),
        Cow::Owned(owned) => Cow::Owned(re.replace_all(&owned, |caps: &Captures| f(caps)).into_owned()),
    }
}

#[inline]
fn replace_hex_blob(caps: &Captures, token: &str) -> String {
    let lead = caps.name("lead").map(|m| m.as_str()).unwrap_or("");
    let trail = caps.name("trail").map(|m| m.as_str()).unwrap_or("");
    let mut out = String::with_capacity(lead.len() + token.len() + trail.len());
    out.push_str(lead);
    out.push_str(token);
    out.push_str(trail);
    out
}

pub fn extract_exception_type(value: &str) -> Option<String> {
    EXCEPTION_RE
        .captures_iter(value)
        .filter_map(|caps| caps.get(1).map(|item| item.as_str().to_string()))
        .last()
}

pub fn determine_error_kind(body: &str, severity_text: &str, exception_type: Option<&str>) -> String {
    // Callers may either pass a pre-extracted exception_type (hot path: scan loop already
    // extracts it once) or pass None and let us extract it from the body (Python wrapper / ad
    // hoc calls). The extracted value is held in a local to preserve borrow lifetime.
    let extracted: Option<String> = if exception_type.is_none() {
        extract_exception_type(body)
    } else {
        None
    };
    let effective = exception_type.or(extracted.as_deref());
    if let Some(kind) = effective {
        return kind.to_string();
    }
    if body.contains("Traceback (most recent call last):") {
        return "traceback".to_string();
    }
    match severity_text {
        "ERROR" | "CRITICAL" | "FATAL" => "log_error".to_string(),
        _ => "none".to_string(),
    }
}

#[inline]
pub fn strip_ansi_cow(value: &str) -> Cow<'_, str> {
    if !value.as_bytes().contains(&0x1b) {
        return Cow::Borrowed(value);
    }
    Cow::Owned(ANSI_RE.replace_all(value, "").into_owned())
}

// Back-compat wrapper used by call-sites that previously expected `&str` directly. We avoid the
// memory-leak path that the old implementation had (`Box::leak`).
pub fn strip_ansi_owned(value: &str) -> String {
    strip_ansi_cow(value).into_owned()
}

pub fn canonical_severity(value: &str) -> &'static str {
    match value {
        "WARN" | "WARNING" => "WARNING",
        "TRACE" => "TRACE",
        "DEBUG" => "DEBUG",
        "INFO" => "INFO",
        "ERROR" => "ERROR",
        "CRITICAL" => "CRITICAL",
        "FATAL" => "FATAL",
        _ => "INFO",
    }
}

pub fn severity_number(value: &str) -> i64 {
    match value {
        "TRACE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARNING" => 13,
        "ERROR" => 17,
        "CRITICAL" | "FATAL" => 21,
        _ => 9,
    }
}

// Byte-level continuation detector. The previous version allocated a `String` and ran regexes
// against it; here we walk the bytes once. The semantics are kept identical:
//   - empty line → continuation
//   - starts with `{` or `[` (after optional whitespace and ANSI) → NOT continuation
//   - starts with an ISO/standard timestamp prefix → NOT continuation
//   - starts with a severity word (DEBUG/INFO/WARNING/ERROR/...) → NOT continuation
//   - otherwise → continuation
pub fn is_continuation_line(line: &str) -> bool {
    is_continuation_bytes(line.as_bytes())
}

pub fn is_continuation_bytes(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return true;
    }
    let trimmed = trim_ansi_and_whitespace(bytes);
    if trimmed.is_empty() {
        return true;
    }
    let first = trimmed[0];
    if first == b'{' || first == b'[' {
        return false;
    }
    if starts_with_timestamp(trimmed) {
        return false;
    }
    if starts_with_level(trimmed) {
        return false;
    }
    true
}

#[inline]
fn trim_ansi_and_whitespace(mut bytes: &[u8]) -> &[u8] {
    loop {
        match bytes.first() {
            Some(b' ') | Some(b'\t') | Some(b'\r') => bytes = &bytes[1..],
            Some(&0x1b) => {
                // Skip ANSI CSI sequence: ESC [ params* letter
                if bytes.len() >= 2 && bytes[1] == b'[' {
                    let mut i = 2;
                    while i < bytes.len() && !bytes[i].is_ascii_alphabetic() {
                        i += 1;
                    }
                    if i < bytes.len() {
                        bytes = &bytes[i + 1..];
                        continue;
                    }
                }
                return bytes;
            }
            _ => return bytes,
        }
    }
}

#[inline]
fn starts_with_timestamp(bytes: &[u8]) -> bool {
    let after_bracket = if bytes.first().copied() == Some(b'[') { &bytes[1..] } else { bytes };
    // YYYY[-/]MM[-/]DD followed by T or space + HH:MM:SS, OR YYYY/MM/DD:HH:MM:SS.
    if after_bracket.len() < 19 {
        return false;
    }
    let ymd_ok = after_bracket[..4].iter().all(|b| b.is_ascii_digit())
        && (after_bracket[4] == b'-' || after_bracket[4] == b'/')
        && after_bracket[5].is_ascii_digit()
        && after_bracket[6].is_ascii_digit()
        && (after_bracket[7] == b'-' || after_bracket[7] == b'/')
        && after_bracket[8].is_ascii_digit()
        && after_bracket[9].is_ascii_digit();
    if !ymd_ok {
        return false;
    }
    let sep = after_bracket[10];
    if !(sep == b'T' || sep == b' ' || sep == b':') {
        return false;
    }
    let rest = &after_bracket[11..];
    rest.len() >= 8
        && rest[0].is_ascii_digit()
        && rest[1].is_ascii_digit()
        && rest[2] == b':'
        && rest[3].is_ascii_digit()
        && rest[4].is_ascii_digit()
        && rest[5] == b':'
        && rest[6].is_ascii_digit()
        && rest[7].is_ascii_digit()
}

#[inline]
fn starts_with_level(bytes: &[u8]) -> bool {
    const LEVELS: &[&[u8]] = &[
        b"TRACE", b"DEBUG", b"INFO", b"WARNING", b"WARN", b"ERROR", b"CRITICAL", b"FATAL",
    ];
    for level in LEVELS {
        if bytes.len() >= level.len() && bytes[..level.len()].eq_ignore_ascii_case(level) {
            let next = bytes.get(level.len()).copied();
            match next {
                None => return true,
                Some(b) if !b.is_ascii_alphanumeric() && b != b'_' => return true,
                _ => continue,
            }
        }
    }
    false
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RustEventRow {
    pub event_id: String,
    pub run_id: String,
    pub source: String,
    pub offset: i64,
    pub line_count: i64,
    pub timestamp: Option<String>,
    pub observed_timestamp: String,
    pub severity_text: String,
    pub severity_number: i64,
    pub parse_format: String,
    pub parse_status: String,
    pub thread_name: Option<String>,
    pub process_name: Option<String>,
    pub process_pid: Option<i64>,
    pub body: String,
    pub normalized_template: String,
    pub error_kind: String,
    pub exception_type: Option<String>,
    pub template_id: String,
    pub raw_hash: String,
    pub classification: String,
    pub classification_reason: Option<String>,
    pub service_name: String,
    pub service_instance_id: Option<String>,
    pub worker_id: Option<String>,
    pub server_kind: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub container_id: Option<String>,
    pub k8s_pod_name: Option<String>,
    pub k8s_container_name: Option<String>,
    pub attributes_json: String,
    pub resource_json: String,
    pub raw: Option<String>,
}

// Per-thread cache that deduplicates expensive work (parse + normalize + exception extraction)
// on identical raw lines. Real captures contain enormous repetition (heartbeats, scheduler
// ticks, kafka keepalives…) so reusing the cached classification cuts a measurable share of CPU.
#[derive(Debug, Clone)]
struct NormalizedCache {
    severity_text: String,
    severity_num: i64,
    normalized_template: String,
    error_kind: String,
    exception_type: Option<String>,
    template_id: String,
    raw_hash: String,
    body: String,
    parsed_meta: ParsedMeta,
}

#[derive(Debug, Clone, Default)]
struct ParsedMeta {
    timestamp: Option<String>,
    thread_name: Option<String>,
    process_name: Option<String>,
    process_pid: Option<i64>,
    service_name: Option<String>,
    service_instance_id: Option<String>,
    worker_id: Option<String>,
    server_kind: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    container_id: Option<String>,
    k8s_pod_name: Option<String>,
    k8s_container_name: Option<String>,
    attributes_json: String,
    resource_json: String,
    parse_format: String,
    parse_status: String,
}

thread_local! {
    // Wrap entries in `Arc` so cache HITs only do an atomic refcount bump
    // instead of cloning the entire NormalizedCache (which has ~20 owned
    // String fields). The downstream `RustEventRow` construction still
    // clones each individual field it consumes — that single clone-per-field
    // is the minimum we owe the Arrow batch builder, and we no longer pay
    // the redundant outer-struct clone on top.
    static DEDUP_CACHE: RefCell<FxHashMap<u128, Arc<NormalizedCache>>> =
        RefCell::new(FxHashMap::with_capacity_and_hasher(8192, Default::default()));
}

pub fn reset_dedup_cache() {
    DEDUP_CACHE.with(|cell| cell.borrow_mut().clear());
}

pub fn parse_logical_record(
    raw: &str,
    service_name: &str,
    run_id: &str,
    source: &str,
    offset: u64,
    line_count: usize,
    observed_timestamp: &str,
    include_raw: bool,
) -> RustEventRow {
    let clean_cow = strip_ansi_cow(raw);
    let clean: &str = clean_cow.as_ref();
    let line_key = xxh3_128_of(clean.as_bytes());

    // Cache stores Arc<NormalizedCache>; `get(..).cloned()` on an
    // Option<&Arc<_>> bumps the Arc refcount rather than deep-cloning the
    // struct. Cache hits no longer pay ~20 String allocations.
    let cached: Option<Arc<NormalizedCache>> =
        DEDUP_CACHE.with(|cell| cell.borrow().get(&line_key).cloned());
    let cache_entry: Arc<NormalizedCache> = match cached {
        Some(entry) => entry,
        None => {
            let entry = Arc::new(build_normalized(clean, service_name));
            DEDUP_CACHE.with(|cell| {
                let mut map = cell.borrow_mut();
                // Light cap to avoid pathological growth on huge captures with no repetition.
                if map.len() >= 200_000 {
                    map.clear();
                }
                map.insert(line_key, Arc::clone(&entry));
            });
            entry
        }
    };

    let event_id = event_hash(run_id, source, offset, &cache_entry.raw_hash);
    let parsed_meta = &cache_entry.parsed_meta;
    let resolved_service = parsed_meta.service_name.clone().unwrap_or_else(|| service_name.to_string());

    RustEventRow {
        event_id,
        run_id: run_id.to_string(),
        source: source.to_string(),
        offset: offset as i64,
        line_count: line_count as i64,
        timestamp: parsed_meta.timestamp.clone(),
        observed_timestamp: observed_timestamp.to_string(),
        severity_text: cache_entry.severity_text.clone(),
        severity_number: cache_entry.severity_num,
        parse_format: parsed_meta.parse_format.clone(),
        parse_status: parsed_meta.parse_status.clone(),
        thread_name: parsed_meta.thread_name.clone(),
        process_name: parsed_meta.process_name.clone(),
        process_pid: parsed_meta.process_pid,
        body: cache_entry.body.clone(),
        normalized_template: cache_entry.normalized_template.clone(),
        error_kind: cache_entry.error_kind.clone(),
        exception_type: cache_entry.exception_type.clone(),
        template_id: cache_entry.template_id.clone(),
        raw_hash: cache_entry.raw_hash.clone(),
        classification: "unclassified".to_string(),
        classification_reason: None,
        service_name: resolved_service,
        service_instance_id: parsed_meta.service_instance_id.clone(),
        worker_id: parsed_meta.worker_id.clone(),
        server_kind: parsed_meta.server_kind.clone(),
        trace_id: parsed_meta.trace_id.clone(),
        span_id: parsed_meta.span_id.clone(),
        container_id: parsed_meta.container_id.clone(),
        k8s_pod_name: parsed_meta.k8s_pod_name.clone(),
        k8s_container_name: parsed_meta.k8s_container_name.clone(),
        attributes_json: parsed_meta.attributes_json.clone(),
        resource_json: parsed_meta.resource_json.clone(),
        raw: if include_raw { Some(clean.to_string()) } else { None },
    }
}

#[inline]
fn xxh3_128_of(bytes: &[u8]) -> u128 {
    let mut hasher = Hash128::with_seed(0);
    hasher.write(bytes);
    xxh3_finish_u128(&hasher)
}

fn build_normalized(clean: &str, service_name: &str) -> NormalizedCache {
    let looks_like_json = clean.as_bytes().iter().find(|&&b| !b.is_ascii_whitespace())
        .map(|b| *b == b'{' || *b == b'[')
        .unwrap_or(false);
    let mut parsed = parse_json_record(clean, service_name)
        .or_else(|| parse_repo_text(clean, service_name))
        .or_else(|| parse_generic_text(clean, service_name))
        .unwrap_or_else(|| ParsedCore {
            timestamp: None,
            severity_text: "INFO".to_string(),
            thread_name: None,
            process_name: None,
            process_pid: None,
            body: clean.trim().to_string(),
            service_name: service_name.to_string(),
            service_instance_id: None,
            worker_id: None,
            server_kind: detect_server_kind(clean, ""),
            trace_id: None,
            span_id: None,
            container_id: None,
            k8s_pod_name: None,
            k8s_container_name: None,
            attributes_json: "{}".to_string(),
            resource_json: "{}".to_string(),
            parse_format: "text".to_string(),
            parse_status: "ok".to_string(),
        });
    if looks_like_json && parsed.parse_format != "json" {
        parsed.parse_status = "degraded".to_string();
    }
    let severity_text = canonical_severity(&parsed.severity_text).to_string();
    let exception_type = extract_exception_type(&parsed.body);
    let error_kind = determine_error_kind(&parsed.body, &severity_text, exception_type.as_deref());
    let normalized_template = normalize_message(&parsed.body);
    // Hash sobre la headline (primera línea normalizada) y SIN error_kind:
    // los tracebacks truncados producen `error_kind` distintos ("traceback"
    // vs "DuplicateKeyError" vs "SMDuplicateException") para el mismo error
    // lógico — meter eso en el hash creaba duplicados artificiales. La
    // headline es estable mientras la causa raíz sea la misma.
    //
    // Fast path: si el body es de una sola línea (caso mayoritario en logs
    // estructurados), `normalized_template` ES la headline normalizada — no
    // hace falta una segunda pasada de regex. Para tracebacks multi-línea
    // necesitamos normalizar sólo la primera línea para mantener un
    // fingerprint estable (`normalize_message(body)` colapsa los newlines y
    // mezcla el traceback completo, que es lo que `headline_fingerprint`
    // intenta cortar). El check es un byte-scan de O(first_line_bytes).
    let fingerprint = if memchr::memchr(b'\n', parsed.body.as_bytes()).is_none() {
        headline_fingerprint_cut(normalized_template.trim())
    } else {
        headline_fingerprint(&parsed.body)
    };
    let template_id = template_hash(service_name, &severity_text, &fingerprint, "");
    let raw_hash = raw_hash_bytes(clean.as_bytes());

    NormalizedCache {
        severity_text: severity_text.clone(),
        severity_num: severity_number(&severity_text),
        normalized_template,
        error_kind,
        exception_type,
        template_id,
        raw_hash,
        body: parsed.body.clone(),
        parsed_meta: ParsedMeta {
            timestamp: parsed.timestamp,
            thread_name: parsed.thread_name,
            process_name: parsed.process_name,
            process_pid: parsed.process_pid,
            service_name: Some(parsed.service_name),
            service_instance_id: parsed.service_instance_id,
            worker_id: parsed.worker_id,
            server_kind: parsed.server_kind,
            trace_id: parsed.trace_id,
            span_id: parsed.span_id,
            container_id: parsed.container_id,
            k8s_pod_name: parsed.k8s_pod_name,
            k8s_container_name: parsed.k8s_container_name,
            attributes_json: parsed.attributes_json,
            resource_json: parsed.resource_json,
            parse_format: parsed.parse_format,
            parse_status: parsed.parse_status,
        },
    }
}

#[derive(Debug, Clone)]
struct ParsedCore {
    timestamp: Option<String>,
    severity_text: String,
    thread_name: Option<String>,
    process_name: Option<String>,
    process_pid: Option<i64>,
    body: String,
    service_name: String,
    service_instance_id: Option<String>,
    worker_id: Option<String>,
    server_kind: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    container_id: Option<String>,
    k8s_pod_name: Option<String>,
    k8s_container_name: Option<String>,
    attributes_json: String,
    resource_json: String,
    parse_format: String,
    parse_status: String,
}

fn parse_repo_text(raw: &str, service_name: &str) -> Option<ParsedCore> {
    let mut parts = raw.splitn(5, '\t');
    let time = parts.next()?.to_string();
    let level_thread_process = parts.next()?;
    let location = parts.next()?.to_string();
    let func = parts.next()?.to_string();
    let message = parts.next()?.trim().to_string();

    let mut ltp = level_thread_process.split_whitespace();
    let level = ltp.next()?.to_string();
    let thread = ltp.next().map(|s| s.to_string());
    let process = ltp.next().map(|s| s.to_string());
    let mut attributes = Map::new();
    attributes.insert("code.location".to_string(), Value::String(location));
    attributes.insert("code.function".to_string(), Value::String(func));
    Some(ParsedCore {
        timestamp: Some(time),
        severity_text: level,
        thread_name: thread,
        process_name: process,
        process_pid: None,
        body: message,
        service_name: service_name.to_string(),
        service_instance_id: None,
        worker_id: None,
        server_kind: detect_server_kind(raw, service_name),
        trace_id: None,
        span_id: None,
        container_id: None,
        k8s_pod_name: None,
        k8s_container_name: None,
        attributes_json: Value::Object(attributes).to_string(),
        resource_json: "{}".to_string(),
        parse_format: "text".to_string(),
        parse_status: "ok".to_string(),
    })
}

fn parse_generic_text(raw: &str, service_name: &str) -> Option<ParsedCore> {
    let caps = GENERIC_TEXT_RE.captures(raw)?;
    Some(ParsedCore {
        timestamp: caps.name("time").map(|m| m.as_str().to_string()),
        severity_text: caps.name("level")?.as_str().to_string(),
        thread_name: None,
        process_name: None,
        process_pid: None,
        body: caps
            .name("message")
            .map(|m| m.as_str().trim_matches(|c| c == ' ' || c == '\t' || c == '-').to_string())
            .unwrap_or_default(),
        service_name: service_name.to_string(),
        service_instance_id: None,
        worker_id: None,
        server_kind: detect_server_kind(raw, service_name),
        trace_id: None,
        span_id: None,
        container_id: None,
        k8s_pod_name: None,
        k8s_container_name: None,
        attributes_json: "{}".to_string(),
        resource_json: "{}".to_string(),
        parse_format: "text".to_string(),
        parse_status: "ok".to_string(),
    })
}

fn parse_json_record(raw: &str, default_service_name: &str) -> Option<ParsedCore> {
    let trimmed = raw.trim_start();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return None;
    }
    // sonic-rs is a SIMD-accelerated JSON parser with the same data model surface we need.
    let value: SonicValue = sonic_rs::from_str(raw).ok()?;
    let obj = value.as_object()?;
    let resource = obj.get(&"resource").and_then(|v| v.as_object());
    let attributes = obj.get(&"attributes").and_then(|v| v.as_object());
    let body = first_string_sonic(&[
        obj.get(&"message"),
        obj.get(&"body"),
        obj.get(&"msg"),
        obj.get(&"event"),
        attributes.as_ref().and_then(|a| a.get(&"message")),
        sonic_value_by_path(obj, "exception.message"),
    ])
    .unwrap_or_else(|| sonic_value_to_string(&value).unwrap_or_default());
    let service_name = first_string_sonic(&[
        obj.get(&"microservice"),
        obj.get(&"service.name"),
        obj.get(&"service_name"),
        obj.get(&"service"),
        resource.as_ref().and_then(|r| r.get(&"service.name")),
        attributes.as_ref().and_then(|a| a.get(&"service.name")),
    ])
    .unwrap_or_else(|| default_service_name.to_string());
    Some(ParsedCore {
        timestamp: first_string_sonic(&[
            obj.get(&"time"),
            obj.get(&"timestamp"),
            obj.get(&"@timestamp"),
            obj.get(&"asctime"),
            obj.get(&"datetime"),
            attributes.as_ref().and_then(|a| a.get(&"time")),
        ]),
        severity_text: first_string_sonic(&[
            obj.get(&"level"),
            obj.get(&"levelname"),
            obj.get(&"severity"),
            obj.get(&"severity_text"),
            obj.get(&"log.level"),
            attributes.as_ref().and_then(|a| a.get(&"level")),
        ])
        .unwrap_or_else(|| "INFO".to_string()),
        thread_name: first_string_sonic(&[
            obj.get(&"threadName"),
            obj.get(&"thread.name"),
            obj.get(&"thread_name"),
            attributes.as_ref().and_then(|a| a.get(&"thread.name")),
        ]),
        process_name: first_string_sonic(&[
            obj.get(&"processName"),
            obj.get(&"process.name"),
            obj.get(&"process_name"),
            attributes.as_ref().and_then(|a| a.get(&"process.name")),
        ]),
        process_pid: first_i64_sonic(&[
            obj.get(&"pid"),
            obj.get(&"process.pid"),
            obj.get(&"process_pid"),
            attributes.as_ref().and_then(|a| a.get(&"process.pid")),
        ]),
        body,
        service_name,
        service_instance_id: first_string_sonic(&[
            obj.get(&"service.instance.id"),
            obj.get(&"service_instance_id"),
            resource.as_ref().and_then(|r| r.get(&"service.instance.id")),
            attributes.as_ref().and_then(|a| a.get(&"service.instance.id")),
        ]),
        worker_id: first_string_sonic(&[
            obj.get(&"worker_id"),
            attributes.as_ref().and_then(|a| a.get(&"worker_id")),
        ]),
        server_kind: first_string_sonic(&[
            obj.get(&"server.kind"),
            obj.get(&"server_kind"),
            resource.as_ref().and_then(|r| r.get(&"server.kind")),
            attributes.as_ref().and_then(|a| a.get(&"server.kind")),
        ])
        .or_else(|| detect_server_kind(raw, default_service_name)),
        trace_id: first_string_sonic(&[
            obj.get(&"trace_id"),
            obj.get(&"traceId"),
            obj.get(&"trace.id"),
        ]),
        span_id: first_string_sonic(&[
            obj.get(&"span_id"),
            obj.get(&"spanId"),
            obj.get(&"span.id"),
        ]),
        container_id: first_string_sonic(&[
            obj.get(&"container.id"),
            obj.get(&"container_id"),
        ]),
        k8s_pod_name: first_string_sonic(&[
            obj.get(&"k8s.pod.name"),
            obj.get(&"pod_name"),
        ]),
        k8s_container_name: first_string_sonic(&[
            obj.get(&"k8s.container.name"),
            obj.get(&"container.name"),
            obj.get(&"k8s_container_name"),
        ]),
        attributes_json: attributes
            .as_ref()
            .map(|a| sonic_rs::to_string(&a).unwrap_or_else(|_| "{}".to_string()))
            .unwrap_or_else(|| "{}".to_string()),
        resource_json: resource
            .as_ref()
            .map(|r| sonic_rs::to_string(&r).unwrap_or_else(|_| "{}".to_string()))
            .unwrap_or_else(|| "{}".to_string()),
        parse_format: "json".to_string(),
        parse_status: "ok".to_string(),
    })
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RustTemplateRow {
    pub template_id: String,
    pub service_name: String,
    pub severity_text: String,
    pub severity_number: i64,
    pub normalized_template: String,
    pub error_kind: String,
    pub exception_type: Option<String>,
    pub first_seen: Option<String>,
    pub last_seen: Option<String>,
    pub example_event_id: String,
    pub parse_status: String,
    pub classification: String,
    pub classification_reason: Option<String>,
    pub baseline_match: bool,
    pub event_count: usize,
}

/// Incremental template aggregator. Fed one row at a time, lets the caller drop the row
/// immediately after the call instead of buffering Vec<RustEventRow>.
pub struct TemplateAggregator {
    by_id: FxHashMap<String, RustTemplateRow>,
}

impl Default for TemplateAggregator {
    fn default() -> Self {
        Self {
            by_id: FxHashMap::with_capacity_and_hasher(1024, Default::default()),
        }
    }
}

impl TemplateAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, record: &RustEventRow) {
        let entry = self
            .by_id
            .entry(record.template_id.clone())
            .or_insert_with(|| RustTemplateRow {
                template_id: record.template_id.clone(),
                service_name: record.service_name.clone(),
                severity_text: record.severity_text.clone(),
                severity_number: record.severity_number,
                normalized_template: record.normalized_template.clone(),
                error_kind: record.error_kind.clone(),
                exception_type: record.exception_type.clone(),
                first_seen: record
                    .timestamp
                    .clone()
                    .or_else(|| Some(record.observed_timestamp.clone())),
                last_seen: record
                    .timestamp
                    .clone()
                    .or_else(|| Some(record.observed_timestamp.clone())),
                example_event_id: record.event_id.clone(),
                parse_status: record.parse_status.clone(),
                classification: "unclassified".to_string(),
                classification_reason: None,
                baseline_match: false,
                event_count: 0,
            });
        entry.event_count += 1;
        let record_time = record
            .timestamp
            .clone()
            .unwrap_or_else(|| record.observed_timestamp.clone());
        if entry.first_seen.as_ref().map(|v| record_time < *v).unwrap_or(true) {
            entry.first_seen = Some(record_time.clone());
        }
        if entry.last_seen.as_ref().map(|v| record_time > *v).unwrap_or(true) {
            entry.last_seen = Some(record_time);
        }
        if entry.parse_status == "ok" && record.parse_status != "ok" {
            entry.parse_status = record.parse_status.clone();
        }
    }

    pub fn finish(self) -> FxHashMap<String, RustTemplateRow> {
        self.by_id
    }

    /// Merge another aggregator (typically a per-chunk one) into `self`.
    pub fn merge(&mut self, other: FxHashMap<String, RustTemplateRow>) {
        for (tid, incoming) in other {
            match self.by_id.get_mut(&tid) {
                None => {
                    self.by_id.insert(tid, incoming);
                }
                Some(existing) => {
                    existing.event_count += incoming.event_count;
                    match (existing.first_seen.as_deref(), incoming.first_seen.as_deref()) {
                        (Some(a), Some(b)) if b < a => existing.first_seen = incoming.first_seen,
                        (None, Some(_)) => existing.first_seen = incoming.first_seen,
                        _ => {}
                    }
                    match (existing.last_seen.as_deref(), incoming.last_seen.as_deref()) {
                        (Some(a), Some(b)) if b > a => existing.last_seen = incoming.last_seen,
                        (None, Some(_)) => existing.last_seen = incoming.last_seen,
                        _ => {}
                    }
                    if existing.parse_status == "ok" && incoming.parse_status != "ok" {
                        existing.parse_status = incoming.parse_status;
                    }
                }
            }
        }
    }
}

pub fn sort_templates(map: FxHashMap<String, RustTemplateRow>) -> Vec<RustTemplateRow> {
    let mut rows: Vec<_> = map.into_values().collect();
    rows.sort_by(|left, right| {
        right
            .event_count
            .cmp(&left.event_count)
            .then_with(|| left.template_id.cmp(&right.template_id))
    });
    rows
}

pub fn aggregate_templates(records: &[RustEventRow]) -> Vec<RustTemplateRow> {
    let mut by_id: FxHashMap<String, RustTemplateRow> =
        FxHashMap::with_capacity_and_hasher(1024, Default::default());
    for record in records {
        let entry = by_id.entry(record.template_id.clone()).or_insert_with(|| RustTemplateRow {
            template_id: record.template_id.clone(),
            service_name: record.service_name.clone(),
            severity_text: record.severity_text.clone(),
            severity_number: record.severity_number,
            normalized_template: record.normalized_template.clone(),
            error_kind: record.error_kind.clone(),
            exception_type: record.exception_type.clone(),
            first_seen: record.timestamp.clone().or_else(|| Some(record.observed_timestamp.clone())),
            last_seen: record.timestamp.clone().or_else(|| Some(record.observed_timestamp.clone())),
            example_event_id: record.event_id.clone(),
            parse_status: record.parse_status.clone(),
            classification: "unclassified".to_string(),
            classification_reason: None,
            baseline_match: false,
            event_count: 0,
        });
        entry.event_count += 1;
        let record_time = record.timestamp.clone().unwrap_or_else(|| record.observed_timestamp.clone());
        if entry.first_seen.as_ref().map(|value| record_time < *value).unwrap_or(true) {
            entry.first_seen = Some(record_time.clone());
        }
        if entry.last_seen.as_ref().map(|value| record_time > *value).unwrap_or(true) {
            entry.last_seen = Some(record_time);
        }
        if entry.parse_status == "ok" && record.parse_status != "ok" {
            entry.parse_status = record.parse_status.clone();
        }
    }
    let mut rows: Vec<_> = by_id.into_values().collect();
    rows.sort_by(|left, right| {
        right
            .event_count
            .cmp(&left.event_count)
            .then_with(|| left.template_id.cmp(&right.template_id))
    });
    rows
}

fn detect_server_kind(raw: &str, source: &str) -> Option<String> {
    let haystack = format!("{} {}", raw.to_lowercase(), source.to_lowercase());
    for value in ["gunicorn", "granian", "kafka", "mongo", "mongodb", "chroot"] {
        if haystack.contains(value) {
            return Some(if value == "mongodb" { "mongo" } else { value }.to_string());
        }
    }
    None
}

fn first_string_sonic(values: &[Option<&SonicValue>]) -> Option<String> {
    values.iter().find_map(|item| item.and_then(|v| sonic_value_to_string(v)))
}

fn first_i64_sonic(values: &[Option<&SonicValue>]) -> Option<i64> {
    values.iter().find_map(|item| item.and_then(|v| sonic_value_to_i64(v)))
}

fn sonic_value_to_string(value: &SonicValue) -> Option<String> {
    if value.is_null() {
        return None;
    }
    if let Some(text) = value.as_str() {
        if text.is_empty() {
            return None;
        }
        return Some(text.to_string());
    }
    if let Some(number) = value.as_i64() {
        return Some(number.to_string());
    }
    if let Some(number) = value.as_f64() {
        return Some(number.to_string());
    }
    if let Some(flag) = value.as_bool() {
        return Some(flag.to_string());
    }
    sonic_rs::to_string(value).ok()
}

fn sonic_value_to_i64(value: &SonicValue) -> Option<i64> {
    if let Some(n) = value.as_i64() {
        return Some(n);
    }
    if let Some(text) = value.as_str() {
        return text.parse::<i64>().ok();
    }
    None
}

fn sonic_value_by_path<'a>(root: &'a sonic_rs::Object, path: &str) -> Option<&'a SonicValue> {
    if let Some(current) = root.get(&path) {
        return Some(current);
    }
    let mut iter = path.split('.');
    let head = iter.next()?;
    let mut current: &SonicValue = root.get(&head)?;
    for part in iter {
        current = current.as_object()?.get(&part)?;
    }
    Some(current)
}

static ANSI_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\x1b\[[0-9;]*m").unwrap());
static EMAIL_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b").unwrap());
static URL_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r#"\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s"']+"#).unwrap());
static UUID_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b").unwrap());
static IPV4_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(?:\.(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}").unwrap());
static IPV6_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b(?:[0-9a-fA-F]{1,4}:){2,7}[0-9a-fA-F]{1,4}\b").unwrap());
static ISO_TS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b\d{4}-\d{2}-\d{2}[T ][0-2]\d:[0-5]\d:[0-5]\d(?:[.,]\d+)?(?:Z|[+-][0-2]\d:?[0-5]\d)?\b").unwrap());
static OLD_TS_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b\d{4}/\d{2}/\d{2}:[0-2]\d:[0-5]\d:[0-5]\d\b").unwrap());
// Hex-blob regexes capture the surrounding non-hex byte (or BOS/EOS) so the replacement
// closure can re-emit it. Avoids `\b`, which treats `_` as a word char and therefore
// fails to match `prefix_<hex>` tokens like `dev_e2e_e668462324a4136c`.
static OBJECT_ID_RE: Lazy<Regex> = Lazy::new(|| Regex::new(
    r"(?P<lead>^|[^0-9a-fA-F])(?P<id>[0-9a-fA-F]{24})(?P<trail>$|[^0-9a-fA-F])"
).unwrap());
static TRACE_ID_RE: Lazy<Regex> = Lazy::new(|| Regex::new(
    r"(?P<lead>^|[^0-9a-fA-F])(?P<id>[0-9a-fA-F]{32})(?P<trail>$|[^0-9a-fA-F])"
).unwrap());
static SPAN_ID_RE: Lazy<Regex> = Lazy::new(|| Regex::new(
    r"(?P<lead>^|[^0-9a-fA-F])(?P<id>[0-9a-fA-F]{16})(?P<trail>$|[^0-9a-fA-F])"
).unwrap());
static HEX_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b0x[0-9a-fA-F]+\b").unwrap());
static PORT_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?P<prefix>\bport[=: ]|:)(?P<port>\d{2,5})(?P<suffix>/?)").unwrap());
static PATH_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"/(?:[A-Za-z0-9._-]+/)+[A-Za-z0-9._-]+").unwrap());
static SPACE_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+").unwrap());
static EXCEPTION_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\b([A-Z][A-Za-z0-9_]*(?:Error|Exception|Timeout|Failure))\b").unwrap());
// Marcadores típicos de "contenido variable" usados por logs estructurados:
// `payload {...}`, `body=...`, `data: ...`, `args: (...)`, etc. Permiten
// cortar la headline antes del payload variable para que el fingerprint sea
// estable entre llamadas.
static HEADLINE_CUT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)\b(?:payload|body|data|context|args|extra|details|metadata|response|request|message|content|kwargs|params)\s*[:=\[\{(]"
    )
    .unwrap()
});
static GENERIC_TEXT_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"^(?:\[)?(?P<time>\d{4}[-/]\d{2}[-/]\d{2}(?:[T ][0-2]\d:[0-5]\d:[0-5]\d(?:[.,]\d+)?(?:Z|[+-][0-2]\d:?[0-5]\d)?|:[0-2]\d:[0-5]\d:[0-5]\d))(?:\])?[\s\t-]+(?P<level>TRACE|DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|FATAL)\b(?P<message>.*)$",
    )
    .unwrap()
});

fn replace_numbers(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < chars.len() {
        let prev = if i == 0 { None } else { Some(chars[i - 1]) };
        let current = chars[i];
        let starts_number = current.is_ascii_digit()
            || (current == '-'
                && i + 1 < chars.len()
                && chars[i + 1].is_ascii_digit()
                && !is_word_or_dot(prev));

        if !starts_number || is_word_or_dot(prev) {
            out.push(current);
            i += 1;
            continue;
        }

        let start = i;
        if chars[i] == '-' {
            i += 1;
        }
        while i < chars.len() && chars[i].is_ascii_digit() {
            i += 1;
        }
        if i < chars.len() && chars[i] == '.' && i + 1 < chars.len() && chars[i + 1].is_ascii_digit() {
            i += 1;
            while i < chars.len() && chars[i].is_ascii_digit() {
                i += 1;
            }
        }

        let next = if i < chars.len() { Some(chars[i]) } else { None };
        if is_word_or_dot(next) {
            out.extend(chars[start..i].iter());
        } else {
            out.push_str("<NUM>");
        }
    }
    out
}

fn is_word_or_dot(value: Option<char>) -> bool {
    matches!(value, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.')
}

#[cfg(test)]
mod entropy_mask_tests {
    use super::{entropy_mask, mask_opaque_token, normalize_message};

    #[test]
    fn masks_word_underscore_hex() {
        // Conversation IDs with arbitrary hex suffix length.
        assert_eq!(mask_opaque_token("conv_50a1442a002b").as_deref(), Some("conv_<HEX>"));
        assert_eq!(mask_opaque_token("conv_deadbeef").as_deref(), Some("conv_<HEX>"));
        assert_eq!(mask_opaque_token("obj_abcdef0123456789").as_deref(), Some("obj_<HEX>"));
    }

    #[test]
    fn keeps_benign_word_underscore_combos() {
        // Service / instance names with low cardinality digits.
        assert_eq!(mask_opaque_token("nsclient_1"), None);
        assert_eq!(mask_opaque_token("mongo_v2"), None);
        // word_word: no digits in suffix → keep.
        assert_eq!(mask_opaque_token("customconfig_profile"), None);
        // Too-short suffix (<6 chars).
        assert_eq!(mask_opaque_token("dev_a3f9"), None);
    }

    #[test]
    fn masks_bare_opaque_ids() {
        // ≥16 chars, mixed alpha+digit, alnum only.
        assert_eq!(mask_opaque_token("aBc123Def456GhI78"), Some("<ID>".into()));
        // base62-ish.
        assert_eq!(mask_opaque_token("k4F2pQz8Mn1Rt7Wx9Vh"), Some("<ID>".into()));
    }

    #[test]
    fn keeps_short_or_pure_word_tokens() {
        assert_eq!(mask_opaque_token("error500"), None);          // <16 chars
        assert_eq!(mask_opaque_token("ConfluentEventBusConsumer"), None); // no digit
        assert_eq!(mask_opaque_token("12345678901234567"), None);  // no alpha
        assert_eq!(mask_opaque_token("customconfig"), None);       // no digit, all alpha
        assert_eq!(mask_opaque_token("python3"), None);            // too short
    }

    #[test]
    fn entropy_mask_inline_in_sentence() {
        let masked = entropy_mask("Conversation was not found conv_50a1442a002b in cache");
        assert_eq!(masked, "Conversation was not found conv_<HEX> in cache");
    }

    #[test]
    fn entropy_mask_preserves_punctuation_and_spaces() {
        let masked = entropy_mask("user=alice id=conv_deadbeef99 status=ok");
        assert_eq!(masked, "user=alice id=conv_<HEX> status=ok");
    }

    #[test]
    fn normalize_pipeline_dedups_conv_ids() {
        // End-to-end: two messages differing only in conv ID must produce the
        // same normalized template, which is exactly the win we want.
        let a = normalize_message("Conversation was not found conv_50a1442a002b");
        let b = normalize_message("Conversation was not found conv_8c3d2e1f4a09");
        assert_eq!(a, b, "expected same normalization, got:\n  a={a}\n  b={b}");
    }

    #[test]
    fn normalize_pipeline_keeps_distinct_prefixes() {
        // Different prefixes must remain distinguishable post-normalization.
        let a = normalize_message("Lookup failed for conv_50a1442a002b");
        let b = normalize_message("Lookup failed for req_50a1442a002b");
        assert_ne!(a, b);
    }
}

// pyo3 in-process binding. When the user installs the `logs_reaper_core` Python module via
// `maturin develop --release --features python` (see pyproject.toml in the Rust crate dir), the
// Python management layer drives the streaming pipeline via `scan_file_to_parquet()`: Rust
// writes the events and templates parquets directly while parsing, so neither side ever holds
// a full events table in memory and no Arrow IPC roundtrip is paid.
#[cfg(feature = "python")]
mod pybinding {
    use super::*;
    use crate::columnar;
    use arrow::record_batch::RecordBatch;
    use memchr::memchr_iter;
    use memmap2::Mmap;
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyModule, PyModuleMethods};
    use std::fs::File;
    use std::path::PathBuf;
    use std::time::Instant;

    #[pyfunction]
    fn template_hash_py(service: &str, severity: &str, normalized_template: &str, error_kind: &str) -> String {
        template_hash(service, severity, normalized_template, error_kind)
    }

    #[pyfunction]
    fn normalize_message_py(value: &str) -> String {
        crate::normalize_message(value)
    }

    #[pyfunction]
    fn headline_fingerprint_py(body: &str) -> String {
        crate::headline_fingerprint(body)
    }

    #[pyfunction]
    fn strip_ansi_py(value: &str) -> String {
        crate::strip_ansi_owned(value)
    }

    #[pyfunction]
    fn extract_exception_type_py(value: &str) -> Option<String> {
        crate::extract_exception_type(value)
    }

    #[pyfunction]
    #[pyo3(signature = (body, severity_text="", exception_type=None))]
    fn determine_error_kind_py(body: &str, severity_text: &str, exception_type: Option<&str>) -> String {
        crate::determine_error_kind(body, severity_text, exception_type)
    }

    // ------------------------------------------------------------------
    // Drain phase: single FFI call, end-to-end in Rust
    // ------------------------------------------------------------------
    //
    // The Python orchestrator just passes file paths; everything else —
    // loading the Drain JSON state, mining over templates, persisting the
    // updated tree, rewriting events.arrow with the remapped template_id
    // dictionary, writing the merged templates.arrow — runs in Rust. The
    // events table only crosses Python's awareness as two file paths.
    //
    // We deliberately do NOT expose the underlying Drainer struct or the
    // bulk remap helper to Python: their only consumer was the (now
    // removed) Python orchestrator, and they were performance traps that
    // pulled per-template iteration into Python.

    use crate::drain::TemplateInput;

    #[pyfunction]
    #[pyo3(signature = (
        templates, drain_state,
        events_ipc=None, events_out=None,
        depth=4, sim_th=0.5, max_children=100
    ))]
    #[allow(clippy::too_many_arguments)]
    fn apply_drain_phase_py<'py>(
        py: Python<'py>,
        templates: Vec<(String, String, i64)>,
        drain_state: &str,
        events_ipc: Option<&str>,
        events_out: Option<&str>,
        depth: usize,
        sim_th: f64,
        max_children: usize,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyDict>, Vec<Bound<'py, PyDict>>)> {
        let drain_state_path = PathBuf::from(drain_state);
        let events_ipc_path = events_ipc.map(PathBuf::from);
        let events_out_path = events_out.map(PathBuf::from);

        let drain_inputs: Vec<TemplateInput> = templates
            .into_iter()
            .map(|(template_id, normalized_template, event_count)| TemplateInput {
                template_id,
                normalized_template,
                event_count: event_count.max(0) as u64,
            })
            .collect();

        let result = py
            .allow_threads(|| {
                crate::drain_phase::apply_drain_phase(
                    events_ipc_path.as_deref(),
                    events_out_path.as_deref(),
                    &drain_state_path,
                    drain_inputs,
                    depth,
                    sim_th,
                    max_children,
                )
            })
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;

        let summary = PyDict::new(py);
        summary.set_item("elapsed_ms", result.summary.elapsed_ms)?;
        summary.set_item("input_templates", result.summary.input_templates)?;
        summary.set_item("output_clusters", result.summary.output_clusters)?;
        summary.set_item("merged_template_groups", result.summary.merged_template_groups)?;
        summary.set_item("events_total", result.summary.events_total)?;
        summary.set_item("drain_clusters_total", result.summary.drain_clusters_total)?;
        summary.set_item("drain_state_path", result.summary.drain_state_path)?;
        summary.set_item("events_rewritten", result.summary.events_rewritten)?;

        let remap_dict = PyDict::new(py);
        for (old, new) in result.remap {
            remap_dict.set_item(old, new)?;
        }

        let mut canonical_rows: Vec<Bound<'py, PyDict>> = Vec::with_capacity(result.canonical.len());
        for row in result.canonical {
            let item = PyDict::new(py);
            item.set_item("template_id", row.canonical_template_id)?;
            item.set_item("drain_template", row.drain_template)?;
            item.set_item("event_count", row.event_count)?;
            item.set_item("member_template_ids", row.member_template_ids)?;
            canonical_rows.push(item);
        }

        Ok((summary, remap_dict, canonical_rows))
    }

    /// Stream the full parse + normalize + aggregate pipeline straight into an Arrow IPC
    /// stream file. Rust writes `events_out` incrementally — each per-chunk RecordBatch is
    /// consumed by an `arrow_ipc::writer::StreamWriter` as it arrives over the bounded
    /// channel, so the events table never materialises whole in memory.
    ///
    /// We use the IPC **stream** format (not the file format) because per-chunk RecordBatches
    /// carry per-chunk dictionaries — the file format requires a single fixed dictionary per
    /// field across the whole file, so it bails out on the second batch with
    /// "Dictionary replacement detected". The stream format supports dictionary deltas /
    /// replacement messages, which is exactly the shape rayon produces.
    ///
    /// Arrow IPC is intentionally uncompressed: the on-disk bytes ARE the in-memory Arrow
    /// buffer layout, so Python re-reads the file by memory-mapping it and gets a zero-copy
    /// `pa.Table`. That makes this scan→table boundary essentially free compared with
    /// parquet+zstd (which paid CPU on both write and read).
    /// Templates are aggregated into a HashMap (small) and written after the scan completes.
    /// Returns a summary dict; Python re-reads the .arrow files for downstream phases.
    #[pyfunction]
    #[pyo3(signature = (input, events_out, templates_out, service, run_id="RUST_RUN", observed_timestamp="1970-01-01T00:00:00Z", include_raw=false, start_offset=0))]
    fn scan_file_to_ipc<'py>(
        py: Python<'py>,
        input: &str,
        events_out: &str,
        templates_out: &str,
        service: &str,
        run_id: &str,
        observed_timestamp: &str,
        include_raw: bool,
        start_offset: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        use arrow::ipc::writer::StreamWriter;
        use std::io::BufWriter;

        let path = PathBuf::from(input);
        // We deliberately don't call metadata() here — the file size can grow
        // between metadata() and mmap(), so the only authoritative end-of-input
        // is what mmap() sees. parse_mmap_streaming aligns `start` to the next
        // \n internally and reports the consumed `new_offset` back.
        let start = start_offset;
        let started = Instant::now();
        reset_dedup_cache();

        let events_path = PathBuf::from(events_out);
        let templates_path = PathBuf::from(templates_out);
        if let Some(parent) = events_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            }
        }
        if let Some(parent) = templates_path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            }
        }

        // Producer pushes batches over a bounded channel; the closure below is the consumer.
        // The FileWriter is created lazily on the first batch so it sees the canonical schema
        // produced by the columnar builder. Subsequent batches reuse the writer.
        let events_file = std::fs::File::create(&events_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let mut pending_bw: Option<BufWriter<std::fs::File>> =
            Some(BufWriter::with_capacity(1 << 20, events_file));
        let mut events_writer: Option<StreamWriter<BufWriter<std::fs::File>>> = None;

        let parse_result = py.allow_threads(|| {
            parse_mmap_streaming(
                &path,
                start,
                service,
                run_id,
                &path.to_string_lossy(),
                observed_timestamp,
                include_raw,
                |batch: RecordBatch| -> Result<(), String> {
                    if events_writer.is_none() {
                        let bw = pending_bw
                            .take()
                            .ok_or_else(|| "events BufWriter already consumed".to_string())?;
                        let writer = StreamWriter::try_new(bw, batch.schema().as_ref())
                            .map_err(|e| e.to_string())?;
                        events_writer = Some(writer);
                    }
                    let writer = events_writer.as_mut().unwrap();
                    writer.write(&batch).map_err(|e| e.to_string())?;
                    Ok(())
                },
            )
        });

        let (templates_map, event_count, new_offset) = parse_result
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))?;
        // `new_offset` is the first absolute byte not yet consumed — equivalent to
        // "EOF clamped to last \n we saw". Python persists this and feeds it back
        // as `start_offset` on the next incremental tick.
        let bytes_processed = new_offset.saturating_sub(start);

        // Close events writer (or write an empty file if the input had zero events).
        if let Some(mut writer) = events_writer.take() {
            writer.finish().map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        } else {
            // No batches ever arrived — write an empty Arrow IPC file with the canonical schema
            // so downstream readers don't have to special-case the absent file.
            let schema = columnar::events_schema();
            let file = std::fs::File::create(&events_path)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            let bw = BufWriter::with_capacity(1 << 20, file);
            let mut w = StreamWriter::try_new(bw, schema.as_ref())
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
            w.finish().map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        }

        // Templates are small (bounded by distinct template count in the input, normally
        // << event_count) — a single IPC RecordBatch fits comfortably.
        let templates = crate::sort_templates(templates_map);
        let templates_batch: RecordBatch = columnar::templates_to_batch(&templates);
        let templates_file = std::fs::File::create(&templates_path)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let templates_bw = BufWriter::with_capacity(1 << 20, templates_file);
        let mut templates_writer = StreamWriter::try_new(
            templates_bw,
            templates_batch.schema().as_ref(),
        )
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        templates_writer.write(&templates_batch)
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        templates_writer.finish()
            .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;

        let duration = started.elapsed().as_secs_f64().max(1e-9);
        let input_gigabytes = bytes_processed as f64 / (1024_f64 * 1024_f64 * 1024_f64);

        let summary = PyDict::new(py);
        summary.set_item("engine", "rust")?;
        summary.set_item("service_name", service)?;
        summary.set_item("input_file", path.to_string_lossy().to_string())?;
        summary.set_item("events_arrow", events_path.to_string_lossy().to_string())?;
        summary.set_item("templates_arrow", templates_path.to_string_lossy().to_string())?;
        summary.set_item("event_count", event_count)?;
        summary.set_item("template_count", templates.len())?;
        // Legacy field name kept for backward compat — callers reading `input_bytes`
        // get bytes ACTUALLY processed this run (not the whole-file size, which would
        // be wrong in incremental mode).
        summary.set_item("input_bytes", bytes_processed)?;
        summary.set_item("input_gigabytes", input_gigabytes)?;
        summary.set_item("start_offset", start)?;
        summary.set_item("new_offset", new_offset)?;
        summary.set_item("bytes_processed", bytes_processed)?;
        summary.set_item("scan_duration_seconds", duration)?;
        summary.set_item("throughput_gb_per_second", input_gigabytes / duration)?;
        summary.set_item("events_per_second", event_count as f64 / duration)?;
        summary.set_item("hash_algorithm", "xxh3-128 + blake3-128")?;
        Ok(summary)
    }

    /// True streaming pipeline: rayon workers parse chunks (capped at MAX_CHUNK_BYTES bytes
    /// each) and push their `(RecordBatch, partial_templates, count)` into a bounded channel.
    /// The caller's `on_batch` closure runs on this thread and drains the channel, deciding
    /// what to do with each batch (write to parquet, hand it to pyarrow, count events).
    /// Because the channel is bounded to roughly `n_threads * 2`, in-flight memory is capped
    /// to a handful of small chunks regardless of input size — no `Vec<RecordBatch>` of the
    /// full scan ever materialises in this function.
    ///
    /// `std::thread::scope` keeps the rayon producer thread tied to this stack frame, so the
    /// mmap and the chunk borrows it produces stay alive for the producer's lifetime without
    /// any unsafe lifetime tricks.
    ///
    /// Returns `(merged_templates, total_event_count)` once the channel has been fully drained.
    fn parse_mmap_streaming<F>(
        path: &PathBuf,
        start_offset: u64,
        service: &str,
        run_id: &str,
        source: &str,
        observed_timestamp: &str,
        include_raw: bool,
        mut on_batch: F,
    ) -> Result<(crate::FxHashMap<String, RustTemplateRow>, usize, u64), String>
    where
        F: FnMut(RecordBatch) -> Result<(), String>,
    {
        let file = File::open(path).map_err(|e| e.to_string())?;
        // SAFETY: read-only mmap; never written through.
        let mmap = unsafe { Mmap::map(&file).map_err(|e| e.to_string())? };
        let bytes: &[u8] = &mmap;
        let start_raw = (start_offset as usize).min(bytes.len());
        // When resuming mid-file (incremental mode) we must guarantee `start` lands
        // exactly at the beginning of a logical line. Two cases:
        //   - start_raw == 0: trivially aligned.
        //   - bytes[start_raw - 1] == b'\n': caller fed us an offset they took from
        //     a previous `new_offset` (we always report aligned positions), aligned.
        //   - otherwise: caller passed an arbitrary offset that fell mid-line; skip
        //     forward to the next \n so we don't emit a garbage half-line event.
        let start = if start_raw == 0
            || bytes.get(start_raw.saturating_sub(1)) == Some(&b'\n')
        {
            start_raw
        } else {
            match memchr::memchr(b'\n', &bytes[start_raw..]) {
                Some(rel) => start_raw + rel + 1,
                None => bytes.len(),
            }
        };
        // Truncate to the last newline-terminated line so that any partial line
        // currently being written by `docker logs -f` is left untouched and
        // picked up on the next incremental pass once the writer closes it.
        let end = if start >= bytes.len() {
            start
        } else {
            match memchr::memrchr(b'\n', &bytes[start..]) {
                Some(rel) => start + rel + 1,
                None => start,
            }
        };
        let region = &bytes[start..end];
        let new_offset = end as u64;

        let chunks = split_into_aligned_chunks(region, start);
        let chunk_count = chunks.len();
        let n_threads = rayon::current_num_threads().max(1);
        // The bounded channel is what makes this actually streaming: a slow consumer (parquet
        // writer) backpressures producers so rayon never gets more than `cap` chunks ahead.
        let cap = (n_threads * 2).max(4);
        type ChunkOutput =
            (RecordBatch, crate::FxHashMap<String, RustTemplateRow>, usize);
        let (tx, rx) = crossbeam_channel::bounded::<ChunkOutput>(cap);

        std::thread::scope(|scope| -> Result<(crate::FxHashMap<String, RustTemplateRow>, usize, u64), String> {
            let producer = scope.spawn(|| -> Result<(), String> {
                let result = chunks.into_par_iter().try_for_each_with(tx, |tx, chunk| {
                    reset_dedup_cache();
                    let out = parse_chunk_streaming(
                        chunk.bytes,
                        chunk.absolute_origin,
                        service,
                        run_id,
                        source,
                        observed_timestamp,
                        include_raw,
                    );
                    tx.send(out).map_err(|e| format!("channel closed: {e}"))
                });
                // tx is dropped here when the closure returns, closing the channel and waking
                // up the consumer.
                result
            });

            // Drain on this thread. If `on_batch` errors mid-way, keep pulling from the channel
            // (without invoking the closure again) so producers don't block forever on a full
            // channel; we'll surface the first error after the producer joins.
            let mut merged = crate::TemplateAggregator::new();
            let mut total = 0_usize;
            let mut received = 0usize;
            let mut on_batch_err: Option<String> = None;
            while let Ok((batch, partial, count)) = rx.recv() {
                received += 1;
                if on_batch_err.is_some() {
                    continue;
                }
                total += count;
                merged.merge(partial);
                if let Err(e) = on_batch(batch) {
                    on_batch_err = Some(e);
                }
            }

            producer
                .join()
                .map_err(|_| "producer thread panicked".to_string())?
                .map_err(|e| format!("producer error: {e}"))?;
            if let Some(e) = on_batch_err {
                return Err(e);
            }
            if received != chunk_count {
                return Err(format!("expected {chunk_count} chunks, drained {received}"));
            }
            Ok((merged.finish(), total, new_offset))
        })
    }

    fn parse_chunk_streaming(
        bytes: &[u8],
        absolute_origin: usize,
        service: &str,
        run_id: &str,
        source: &str,
        observed_timestamp: &str,
        include_raw: bool,
    ) -> (RecordBatch, crate::FxHashMap<String, RustTemplateRow>, usize) {
        // Roughly one event per 256 bytes of input (typical service logs); over-allocating the
        // builder once is far cheaper than growing it incrementally.
        let estimated_rows = (bytes.len() / 256).max(64);
        let mut builder = columnar::EventsBatchBuilder::with_capacity(estimated_rows);
        let mut templates = crate::TemplateAggregator::new();
        let mut count = 0usize;

        let mut current_start: Option<usize> = None;
        let mut current_lines: usize = 0;
        let mut line_start: usize = 0;
        let emit = |start_byte: usize,
                        end_byte: usize,
                        lines: usize,
                        builder: &mut columnar::EventsBatchBuilder,
                        templates: &mut crate::TemplateAggregator,
                        count: &mut usize| {
            let raw_bytes = strip_trailing_newline(&bytes[start_byte..end_byte]);
            let raw_str = match std::str::from_utf8(raw_bytes) {
                Ok(s) => std::borrow::Cow::Borrowed(s),
                Err(_) => String::from_utf8_lossy(raw_bytes),
            };
            let offset = (absolute_origin + start_byte) as u64;
            let row = parse_logical_record(
                &raw_str,
                service,
                run_id,
                source,
                offset,
                lines,
                observed_timestamp,
                include_raw,
            );
            templates.observe(&row);
            builder.push(row);
            *count += 1;
        };

        for newline_idx in memchr_iter(b'\n', bytes) {
            let line = strip_trailing_cr(&bytes[line_start..newline_idx]);
            let is_cont = is_continuation_bytes(line) && current_start.is_some();
            if is_cont {
                current_lines += 1;
            } else {
                if let Some(rec_start) = current_start.take() {
                    emit(rec_start, line_start, current_lines, &mut builder, &mut templates, &mut count);
                }
                current_start = Some(line_start);
                current_lines = 1;
            }
            line_start = newline_idx + 1;
        }
        if line_start < bytes.len() {
            let tail = strip_trailing_cr(&bytes[line_start..]);
            let is_cont = is_continuation_bytes(tail) && current_start.is_some();
            if is_cont {
                current_lines += 1;
            } else {
                if let Some(rec_start) = current_start.take() {
                    emit(rec_start, line_start, current_lines, &mut builder, &mut templates, &mut count);
                }
                current_start = Some(line_start);
                current_lines = 1;
            }
            if let Some(rec_start) = current_start.take() {
                emit(rec_start, bytes.len(), current_lines, &mut builder, &mut templates, &mut count);
            }
        } else if let Some(rec_start) = current_start.take() {
            emit(rec_start, line_start, current_lines, &mut builder, &mut templates, &mut count);
        }

        (builder.finish(), templates.finish(), count)
    }

    struct AlignedChunk<'a> {
        bytes: &'a [u8],
        absolute_origin: usize,
    }

    /// Cap a chunk at this many bytes so the in-flight Arrow builder per worker stays bounded
    /// (~8 MiB of source bytes → a few hundred KiB of builder buffers + a small RecordBatch).
    /// On a 1 GiB scan with 32 workers, the old shape produced 32 × 32 MiB chunks all live
    /// simultaneously; with 8 MiB chunks rayon's worker pool only keeps ~n_threads*2 chunks in
    /// flight at any moment, and the bounded channel that consumes RecordBatches drains the
    /// rest to parquet on disk before workers race too far ahead.
    pub(super) const MAX_CHUNK_BYTES: usize = 8 * 1024 * 1024;

    fn split_into_aligned_chunks(region: &[u8], absolute_origin: usize) -> Vec<AlignedChunk<'_>> {
        let n_threads = rayon::current_num_threads().max(1);
        // Below this size the overhead of chunking + rayon dwarfs the parsing work; stay single
        // threaded.
        const MIN_PARALLEL_BYTES: usize = 4 * 1024 * 1024;
        if region.len() < MIN_PARALLEL_BYTES || n_threads <= 1 {
            return vec![AlignedChunk { bytes: region, absolute_origin }];
        }
        // The target chunk size is the smaller of (a) what one thread would parse at equal split
        // and (b) MAX_CHUNK_BYTES. (b) is what keeps the in-flight builder footprint flat as the
        // input grows — bigger inputs produce *more* chunks, not bigger ones.
        let equal_split = region.len() / n_threads;
        let target = equal_split.min(MAX_CHUNK_BYTES).max(1);
        let expected_chunks = (region.len() + target - 1) / target;
        let mut chunks: Vec<AlignedChunk<'_>> = Vec::with_capacity(expected_chunks);
        let mut cursor = 0;
        while cursor < region.len() {
            let approx_end = (cursor + target).min(region.len());
            // Advance to the next newline so the next chunk begins at the start of a line.
            let aligned_end = if approx_end >= region.len() {
                region.len()
            } else {
                match memchr::memchr(b'\n', &region[approx_end..]) {
                    Some(rel) => approx_end + rel + 1,
                    None => region.len(),
                }
            };
            if aligned_end <= cursor {
                break;
            }
            chunks.push(AlignedChunk {
                bytes: &region[cursor..aligned_end],
                absolute_origin: absolute_origin + cursor,
            });
            cursor = aligned_end;
        }
        chunks
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

    #[pymodule]
    fn logs_reaper_core(module: &Bound<'_, PyModule>) -> PyResult<()> {
        module.add_function(wrap_pyfunction!(template_hash_py, module)?)?;
        module.add_function(wrap_pyfunction!(scan_file_to_ipc, module)?)?;
        module.add_function(wrap_pyfunction!(normalize_message_py, module)?)?;
        module.add_function(wrap_pyfunction!(headline_fingerprint_py, module)?)?;
        module.add_function(wrap_pyfunction!(strip_ansi_py, module)?)?;
        module.add_function(wrap_pyfunction!(extract_exception_type_py, module)?)?;
        module.add_function(wrap_pyfunction!(determine_error_kind_py, module)?)?;
        module.add_function(wrap_pyfunction!(apply_drain_phase_py, module)?)?;
        Ok(())
    }
}
