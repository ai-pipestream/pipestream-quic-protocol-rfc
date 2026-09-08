//! interface-v1 event records: append+fsync writer and validating reader.
//!
//! One complete record is one LF-terminated TSV line, written and fsync'd as a
//! unit. A truncated final line, a wrong version, a run/scenario mismatch, or a
//! non-monotonic `seq` within one `process_start_id` all fail validation.

use crate::hex;
use anyhow::{Context, Result, bail, ensure};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};

pub const EVENT_VERSION: &str = "1";
pub const MAX_RECORDS_PER_PROCESS: usize = 65536;
pub const MAX_ARTIFACT_REFERENCES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactRef {
    pub path: String,
    pub len: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub run_id: String,
    pub scenario_id: String,
    pub subject_lang: String,
    pub subject_role: String,
    pub process_start_id: String,
    pub seq: u64,
    pub boundary: String,
    pub operation_id: String,
    pub work_key: String,
    pub attempt: String,
    pub refusal_code: String,
    pub artifact: Option<ArtifactRef>,
}

pub fn escape_label(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            character => escaped.push(character),
        }
    }
    escaped
}

pub fn unescape_label(value: &str) -> Result<String> {
    let mut unescaped = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            unescaped.push(character);
            continue;
        }
        match characters.next() {
            Some('t') => unescaped.push('\t'),
            Some('n') => unescaped.push('\n'),
            Some('r') => unescaped.push('\r'),
            Some('\\') => unescaped.push('\\'),
            other => bail!("invalid escape sequence in label: \\{other:?}"),
        }
    }
    Ok(unescaped)
}

fn decimal_field(name: &str, value: &str, optional: bool) -> Result<()> {
    if value.is_empty() {
        ensure!(optional, "event field {name} must not be empty");
        return Ok(());
    }
    ensure!(
        value.bytes().all(|byte| byte.is_ascii_digit()),
        "event field {name} must be decimal: {value}"
    );
    Ok(())
}

fn hex_field(name: &str, value: &str, digits: usize, optional: bool) -> Result<()> {
    if value.is_empty() {
        ensure!(optional, "event field {name} must not be empty");
        return Ok(());
    }
    ensure!(
        value.len() == digits && value.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "event field {name} must be {digits} lowercase hex digits: {value}"
    );
    ensure!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "event field {name} must be lowercase hex: {value}"
    );
    Ok(())
}

impl Event {
    pub fn line(&self) -> Result<String> {
        ensure!(self.seq >= 1, "event seq starts at 1, got {}", self.seq);
        decimal_field("seq", &self.seq.to_string(), false)?;
        decimal_field("attempt", &self.attempt, true)?;
        decimal_field("refusal_code", &self.refusal_code, true)?;
        hex_field("operation_id", &self.operation_id, 32, true)?;
        hex_field(
            "artifact_sha256",
            self.artifact
                .as_ref()
                .map(|a| a.sha256.as_str())
                .unwrap_or(""),
            64,
            true,
        )?;
        if let Some(artifact) = &self.artifact {
            ensure!(
                !artifact.path.is_empty() && artifact.path.contains('/'),
                "artifact_path must be a non-empty label relative to the scenario directory: {}",
                artifact.path
            );
            ensure!(
                !artifact.path.starts_with('/')
                    && !artifact.path.split('/').any(|part| part == ".."),
                "artifact_path must stay inside the scenario directory: {}",
                artifact.path
            );
        }
        let mut line = String::new();
        line.push_str(EVENT_VERSION);
        for field in [
            &self.run_id,
            &self.scenario_id,
            &self.subject_lang,
            &self.subject_role,
            &self.process_start_id,
        ] {
            line.push('\t');
            line.push_str(&escape_label(field));
        }
        line.push('\t');
        line.push_str(&self.seq.to_string());
        for field in [
            &self.boundary,
            &self.operation_id,
            &self.work_key,
            &self.attempt,
            &self.refusal_code,
        ] {
            line.push('\t');
            line.push_str(&escape_label(field));
        }
        match &self.artifact {
            Some(artifact) => {
                line.push('\t');
                line.push_str(&escape_label(&artifact.path));
                line.push('\t');
                line.push_str(&artifact.len.to_string());
                line.push('\t');
                line.push_str(&artifact.sha256);
            }
            None => line.push_str("\t\t\t"),
        }
        Ok(line)
    }
}

pub struct EventWriter {
    file: File,
    records: usize,
    artifact_refs: usize,
    seq: u64,
    run_id: String,
    scenario_id: String,
    subject_lang: String,
    subject_role: String,
    process_start_id: String,
}

impl EventWriter {
    pub fn open(
        path: &Path,
        run_id: &str,
        scenario_id: &str,
        subject_lang: &str,
        subject_role: &str,
        process_start_id: &str,
    ) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open event record {}", path.display()))?;
        Ok(Self {
            file,
            records: 0,
            artifact_refs: 0,
            seq: 0,
            run_id: run_id.to_owned(),
            scenario_id: scenario_id.to_owned(),
            subject_lang: subject_lang.to_owned(),
            subject_role: subject_role.to_owned(),
            process_start_id: process_start_id.to_owned(),
        })
    }

    /// Append one complete record and fsync it before returning.
    pub fn append(
        &mut self,
        boundary: &str,
        operation_id: Option<[u8; 16]>,
        work_key: Option<&str>,
        attempt: Option<u64>,
        refusal_code: Option<u32>,
        artifact: Option<ArtifactRef>,
    ) -> Result<()> {
        ensure!(
            self.records < MAX_RECORDS_PER_PROCESS,
            "event record bound exceeded: at most {MAX_RECORDS_PER_PROCESS} records per process"
        );
        if artifact.is_some() {
            self.artifact_refs += 1;
            ensure!(
                self.artifact_refs <= MAX_ARTIFACT_REFERENCES,
                "artifact reference bound exceeded: at most {MAX_ARTIFACT_REFERENCES} per scenario"
            );
        }
        self.seq += 1;
        let event = Event {
            run_id: self.run_id.clone(),
            scenario_id: self.scenario_id.clone(),
            subject_lang: self.subject_lang.clone(),
            subject_role: self.subject_role.clone(),
            process_start_id: self.process_start_id.clone(),
            seq: self.seq,
            boundary: boundary.to_owned(),
            operation_id: operation_id.map(|id| hex(&id)).unwrap_or_default(),
            work_key: work_key.unwrap_or_default().to_owned(),
            attempt: attempt.map(|value| value.to_string()).unwrap_or_default(),
            refusal_code: refusal_code
                .map(|value| value.to_string())
                .unwrap_or_default(),
            artifact,
        };
        let mut line = event.line()?;
        line.push('\n');
        self.file.write_all(line.as_bytes())?;
        self.file.sync_all()?;
        self.records += 1;
        Ok(())
    }
}

pub fn read_events(path: &Path, expected_run: &str, expected_scenario: &str) -> Result<Vec<Event>> {
    read_events_checked(path, expected_run, expected_scenario, None)
}

/// Read and fully validate an events file. When `scenario_dir` is given, every
/// referenced artifact must exist inside it.
pub fn read_events_checked(
    path: &Path,
    expected_run: &str,
    expected_scenario: &str,
    scenario_dir: Option<&Path>,
) -> Result<Vec<Event>> {
    let bytes = fs::read(path).with_context(|| format!("read event records {}", path.display()))?;
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        bail!(
            "torn final event line in {}: file does not end with a complete LF-terminated record",
            path.display()
        );
    }
    let text = std::str::from_utf8(&bytes)
        .with_context(|| format!("event records {} are not UTF-8", path.display()))?;
    ensure!(
        !text.contains('\0'),
        "event records {} contain an embedded NUL",
        path.display()
    );
    let mut events = Vec::new();
    let mut last_seq: BTreeMap<String, u64> = BTreeMap::new();
    for (index, raw) in text.lines().enumerate() {
        let location = format!("{} line {}", path.display(), index + 1);
        let fields = raw.split('\t').collect::<Vec<_>>();
        ensure!(
            fields.len() == 15,
            "{location}: expected 15 columns, got {}",
            fields.len()
        );
        ensure!(
            fields[0] == EVENT_VERSION,
            "{location}: unknown event version {}",
            fields[0]
        );
        let run_id = unescape_label(fields[1])?;
        let scenario_id = unescape_label(fields[2])?;
        ensure!(
            run_id == expected_run,
            "{location}: run_id {run_id:?} does not match directory run {expected_run:?}"
        );
        ensure!(
            scenario_id == expected_scenario,
            "{location}: scenario_id {scenario_id:?} does not match directory scenario {expected_scenario:?}"
        );
        let subject_lang = unescape_label(fields[3])?;
        ensure!(
            matches!(subject_lang.as_str(), "rust" | "java"),
            "{location}: invalid subject_lang {subject_lang:?}"
        );
        let subject_role = unescape_label(fields[4])?;
        ensure!(
            matches!(subject_role.as_str(), "server" | "client"),
            "{location}: invalid subject_role {subject_role:?}"
        );
        let process_start_id = unescape_label(fields[5])?;
        let seq: u64 = fields[6]
            .parse()
            .with_context(|| format!("{location}: seq is not a decimal integer"))?;
        let previous = last_seq.get(&process_start_id).copied().unwrap_or(0);
        ensure!(
            seq > previous,
            "{location}: non-monotonic seq {seq} for process_start_id {process_start_id:?} (previous {previous})"
        );
        last_seq.insert(process_start_id.clone(), seq);
        let boundary = unescape_label(fields[7])?;
        let operation_id = fields[8].to_owned();
        hex_field("operation_id", &operation_id, 32, true)
            .with_context(|| format!("{location}: malformed operation_id"))?;
        let work_key = unescape_label(fields[9])?;
        let attempt = fields[10].to_owned();
        decimal_field("attempt", &attempt, true)
            .with_context(|| format!("{location}: malformed attempt"))?;
        let refusal_code = fields[11].to_owned();
        decimal_field("refusal_code", &refusal_code, true)
            .with_context(|| format!("{location}: malformed refusal_code"))?;
        let artifact_path = unescape_label(fields[12])?;
        let artifact_len = fields[13].to_owned();
        let artifact_sha256 = fields[14].to_owned();
        let artifact = if artifact_path.is_empty()
            && artifact_len.is_empty()
            && artifact_sha256.is_empty()
        {
            None
        } else {
            ensure!(
                !artifact_path.is_empty()
                    && !artifact_len.is_empty()
                    && !artifact_sha256.is_empty(),
                "{location}: artifact_path/artifact_len/artifact_sha256 must be all present or all empty"
            );
            let len: u64 = artifact_len
                .parse()
                .with_context(|| format!("{location}: artifact_len is not a decimal integer"))?;
            hex_field("artifact_sha256", &artifact_sha256, 64, false)
                .with_context(|| format!("{location}: malformed artifact_sha256"))?;
            Some(ArtifactRef {
                path: artifact_path.clone(),
                len,
                sha256: artifact_sha256,
            })
        };
        if let (Some(artifact), Some(directory)) = (&artifact, scenario_dir) {
            ensure!(
                !artifact.path.starts_with('/')
                    && !artifact.path.split('/').any(|part| part == ".."),
                "{location}: artifact_path escapes the scenario directory: {}",
                artifact.path
            );
            let full = directory.join(&artifact.path);
            let metadata = fs::metadata(&full).with_context(|| {
                format!("{location}: missing referenced artifact {}", full.display())
            })?;
            ensure!(
                metadata.len() == artifact.len,
                "{location}: artifact {} has length {} but the record claims {}",
                artifact.path,
                metadata.len(),
                artifact.len
            );
        }
        events.push(Event {
            run_id,
            scenario_id,
            subject_lang,
            subject_role,
            process_start_id,
            seq,
            boundary,
            operation_id,
            work_key,
            attempt,
            refusal_code,
            artifact,
        });
    }
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn writer(path: &Path) -> EventWriter {
        EventWriter::open(
            path,
            "run-a",
            "g1-leaf-copy",
            "rust",
            "client",
            "4242-deadbeef",
        )
        .unwrap()
    }

    #[test]
    fn append_and_read_back_round_trip() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.tsv");
        let mut events = writer(&path);
        events
            .append(
                "REQUEST_SENT",
                Some([1; 16]),
                Some("0:0:1"),
                Some(1),
                None,
                None,
            )
            .unwrap();
        events
            .append(
                "RESULT_INSTALLED",
                Some([2; 16]),
                Some("0:0:1"),
                Some(1),
                None,
                Some(ArtifactRef {
                    path: "artifacts/output.bin".into(),
                    len: 3,
                    sha256: "a".repeat(64),
                }),
            )
            .unwrap();
        drop(events);
        let parsed = read_events(&path, "run-a", "g1-leaf-copy").unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].seq, 1);
        assert_eq!(parsed[1].seq, 2);
        assert_eq!(parsed[1].operation_id, "02".repeat(16));
        assert_eq!(
            parsed[1].artifact.as_ref().unwrap().path,
            "artifacts/output.bin"
        );
    }

    #[test]
    fn labels_are_escaped_and_unescaped_unambiguously() {
        assert_eq!(escape_label("a\tb\nc\\d\re"), "a\\tb\\nc\\\\d\\re");
        assert_eq!(
            unescape_label("a\\tb\\nc\\\\d\\re").unwrap(),
            "a\tb\nc\\d\re"
        );
        assert!(unescape_label("bad\\x").is_err());
        assert!(unescape_label("dangling\\").is_err());
    }

    #[test]
    fn nc_torn_event_line_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.tsv");
        let mut events = writer(&path);
        events
            .append("REQUEST_SENT", Some([1; 16]), None, None, None, None)
            .unwrap();
        drop(events);
        let mut torn = fs::read_to_string(&path).unwrap();
        torn.push_str("1\trun-a\tg1-leaf-copy\trust\tclient\t4242-deadbeef\t2\tREQUEST");
        fs::write(&path, torn).unwrap();
        let error = read_events(&path, "run-a", "g1-leaf-copy").unwrap_err();
        assert!(
            error.to_string().contains("torn final event line"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn non_monotonic_seq_within_one_process_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.tsv");
        let mut events = writer(&path);
        events
            .append("REQUEST_SENT", Some([1; 16]), None, None, None, None)
            .unwrap();
        events
            .append("REQUEST_SENT", Some([1; 16]), None, None, None, None)
            .unwrap();
        drop(events);
        let text = fs::read_to_string(&path).unwrap();
        let lines = text.lines().collect::<Vec<_>>();
        let mut fields = lines[1].split('\t').collect::<Vec<_>>();
        fields[6] = "1";
        let replacement = fields.join("\t");
        let mut text = lines[0].to_owned();
        text.push('\n');
        text.push_str(&replacement);
        text.push('\n');
        fs::write(&path, text).unwrap();
        let error = read_events(&path, "run-a", "g1-leaf-copy").unwrap_err();
        assert!(
            error.to_string().contains("non-monotonic seq"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn wrong_version_and_run_scenario_mismatch_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.tsv");
        let mut events = writer(&path);
        events
            .append("REQUEST_SENT", None, None, None, None, None)
            .unwrap();
        drop(events);
        let text = fs::read_to_string(&path).unwrap();
        let wrong_version = text.replacen("1\trun-a\t", "2\trun-a\t", 1);
        fs::write(&path, &wrong_version).unwrap();
        let error = read_events(&path, "run-a", "g1-leaf-copy").unwrap_err();
        assert!(error.to_string().contains("unknown event version"));
        let wrong_run = text.replacen("run-a", "run-b", 1);
        fs::write(&path, wrong_run).unwrap();
        let error = read_events(&path, "run-a", "g1-leaf-copy").unwrap_err();
        assert!(error.to_string().contains("does not match directory run"));
        let wrong_scenario = text.replacen("g1-leaf-copy", "other-scenario", 1);
        fs::write(&path, wrong_scenario).unwrap();
        let error = read_events(&path, "run-a", "g1-leaf-copy").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match directory scenario"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn missing_or_wrong_length_referenced_artifact_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.tsv");
        let mut events = writer(&path);
        events
            .append(
                "RESULT_INSTALLED",
                None,
                None,
                None,
                None,
                Some(ArtifactRef {
                    path: "artifacts/output.bin".into(),
                    len: 5,
                    sha256: hex(&Sha256::digest(b"hello")),
                }),
            )
            .unwrap();
        drop(events);
        let error = read_events_checked(&path, "run-a", "g1-leaf-copy", Some(directory.path()))
            .unwrap_err();
        assert!(error.to_string().contains("missing referenced artifact"));
        let artifacts = directory.path().join("artifacts");
        fs::create_dir_all(&artifacts).unwrap();
        fs::write(artifacts.join("output.bin"), b"hello").unwrap();
        assert_eq!(
            read_events_checked(&path, "run-a", "g1-leaf-copy", Some(directory.path()))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn record_and_artifact_bounds_are_enforced() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.tsv");
        let mut events = writer(&path);
        for _ in 0..MAX_RECORDS_PER_PROCESS {
            events
                .append("REQUEST_SENT", None, None, None, None, None)
                .unwrap();
        }
        let error = events
            .append("REQUEST_SENT", None, None, None, None, None)
            .unwrap_err();
        assert!(error.to_string().contains("event record bound exceeded"));
    }
}
