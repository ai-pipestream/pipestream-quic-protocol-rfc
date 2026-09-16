//! Group-R resource collectors (milestone 16). Black-box measurement of the
//! subject process group and its fixture store, using only /proc, stat(2)
//! metadata, and optional JDK CLI tools — the crate keeps its zero new
//! production dependency budget.
//!
//! Measurement scopes are NEVER substituted: whole-process RSS/HWM, thread
//! count, FD count, actual disk I/O (`/proc/<pid>/io`), Java heap (jstat, only
//! when a JDK ships it), file lengths, and allocated filesystem blocks
//! (`st_blocks`) are separate scopes, and the collection method is recorded
//! per sample line. An unavailable MANDATORY metric fails the row that needs
//! it (never recorded as zero); the optional Java-heap scope degrades to a
//! named gap with RSS still collected.
//!
//! Process-group discovery: the anchor is the fixture-owned server pid. The
//! measured group is the anchor plus every transitive descendant by
//! parent-pid walk through /proc — the subject process tree. This
//! deliberately excludes the fixture driver and the transient client
//! processes it spawns (they share the driver's process group, not the
//! subject tree); a fixture child that re-parents to init after the anchor
//! exits leaves the group, which is correct because measurement windows end
//! before the anchor stops. Every sampled line carries pid/ppid/pgrp so the
//! discovery is auditable after the run.
//!
//! Dead-collector rule: a sampling tick that cannot read a mandatory scope
//! records an `error:` note on that pid's line and increments the summary
//! error count; rows using the collector assert zero errors. The validating
//! reader reads such a line back (its pid columns may be `-`) and surfaces
//! the note, so the ROW fails on it rather than the reader hiding it, and it
//! rejects a torn final line, a wrong field count, or a non-numeric metric,
//! so a truncated resources.tsv can never pass silently.

use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Write},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

/// v2 adds the `cancelled_write_bytes` column after `write_bytes`: without it
/// a large `write_bytes` rate cannot be told apart from page-cache writeback
/// that was cancelled before it ever reached the device, which is a different
/// claim about a subject. The version is in the header so a v1 artifact is
/// never silently read with v2 column offsets.
pub const PROCESS_HEADER: &str = "# pipestream-resources-v2";
/// Column count of one v2 resources.tsv record.
pub const PROCESS_FIELDS: usize = 14;
pub const STORE_HEADER: &str = "# pipestream-store-v1";
/// Default sampling interval (100 ms per the matrix measurement rules).
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
/// Cadence of the optional Java-heap scope. Each probe launches `jstat`,
/// which is itself a JVM, so it runs an order of magnitude less often than
/// the /proc scopes; heap is absent (`-`) on the other ticks, never zero, and
/// the ticks that did collect it carry a `heap:jstat` method note.
pub const HEAP_SAMPLE_INTERVAL: Duration = Duration::from_millis(1000);

// ---------------------------------------------------------------------------
// Host facts and capability manifest
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostFacts {
    pub os_type: String,
    pub kernel_release: String,
    pub cpu_count: usize,
    pub mem_total_kb: u64,
    /// Filesystem type backing the fixture root (from /proc/self/mounts).
    pub fixture_fs_type: String,
    pub fixture_mount: String,
    pub fixture_device: String,
}

fn read_trimmed(path: &Path) -> Result<String> {
    let mut text = String::new();
    File::open(path)
        .with_context(|| format!("open {}", path.display()))?
        .read_to_string(&mut text)
        .with_context(|| format!("read {}", path.display()))?;
    Ok(text.trim().to_owned())
}

/// One line of /proc/self/mounts: (device, mount_point, fs_type).
fn parse_mounts(text: &str) -> Vec<(String, String, String)> {
    text.lines()
        .filter_map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() >= 3 {
                Some((
                    fields[0].to_owned(),
                    fields[1].replace("\\040", " "),
                    fields[2].to_owned(),
                ))
            } else {
                None
            }
        })
        .collect()
}

/// Host facts for the capability-manifest row. `fixture_root` is the
/// directory whose backing filesystem is reported.
pub fn host_facts(fixture_root: &Path) -> Result<HostFacts> {
    let os_type = read_trimmed(Path::new("/proc/sys/kernel/ostype"))?;
    let kernel_release = read_trimmed(Path::new("/proc/sys/kernel/osrelease"))?;
    let cpuinfo =
        fs::read_to_string("/proc/cpuinfo").context("read /proc/cpuinfo for the host CPU count")?;
    let cpu_count = cpuinfo
        .lines()
        .filter(|l| l.starts_with("processor"))
        .count();
    let cpu_count = cpu_count.max(1);
    let meminfo = fs::read_to_string("/proc/meminfo").context("read /proc/meminfo")?;
    let mem_total_kb = meminfo
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            match (fields.next(), fields.next()) {
                (Some("MemTotal:"), Some(value)) => value.parse::<u64>().ok(),
                _ => None,
            }
        })
        .context("MemTotal not present in /proc/meminfo")?;
    let canonical = fs::canonicalize(fixture_root)
        .with_context(|| format!("canonicalize fixture root {}", fixture_root.display()))?;
    let mounts = parse_mounts(&fs::read_to_string("/proc/self/mounts").context("read mounts")?);
    let chosen = mounts
        .iter()
        .filter(|(_, mount, _)| canonical.starts_with(mount))
        .max_by_key(|(_, mount, _)| mount.len());
    let (fixture_device, fixture_mount, fixture_fs_type) = chosen
        .map(|(device, mount, fs_type)| (device.clone(), mount.clone(), fs_type.clone()))
        .unwrap_or_else(|| {
            (
                "unknown".to_owned(),
                "unknown".to_owned(),
                "unknown (mount not matched)".to_owned(),
            )
        });
    Ok(HostFacts {
        os_type,
        kernel_release,
        cpu_count,
        mem_total_kb,
        fixture_fs_type,
        fixture_mount,
        fixture_device,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcPermissions {
    pub io: bool,
    pub status: bool,
    pub fd: bool,
    pub net_dev: bool,
}

/// Which /proc scopes are readable for `pid` right now. Rows treat a false
/// MANDATORY scope as an unmet measurement gate (the row fails: an unavailable
/// mandatory metric is never recorded as zero).
pub fn proc_permissions(pid: u32) -> ProcPermissions {
    let base = format!("/proc/{pid}");
    ProcPermissions {
        io: File::open(format!("{base}/io")).is_ok(),
        status: File::open(format!("{base}/status")).is_ok(),
        fd: std::fs::read_dir(format!("{base}/fd")).is_ok(),
        net_dev: File::open("/proc/net/dev").is_ok(),
    }
}

/// Locate a tool on PATH (no external crate): first executable match wins.
pub fn tool_on_path(name: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path_var) {
        let candidate = directory.join(name);
        if candidate.is_file()
            && fs::metadata(&candidate)
                .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
        {
            return Some(candidate);
        }
    }
    None
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Calibration {
    pub samples: usize,
    pub total_ns: u128,
    pub per_sample_ns: u128,
}

/// Collector overhead: the wall time to collect N full samples of one pid.
/// Recorded in the capability manifest so every later row can state its
/// measurement overhead.
pub fn calibrate(anchor: u32, samples: usize) -> Result<Calibration> {
    ensure!(samples > 0, "calibration needs at least one sample");
    let start = Instant::now();
    for _ in 0..samples {
        let _ = sample_pid(anchor, false)?;
    }
    let total_ns = start.elapsed().as_nanos();
    Ok(Calibration {
        samples,
        total_ns,
        per_sample_ns: total_ns / samples as u128,
    })
}

// ---------------------------------------------------------------------------
// /proc parsing (unit-tested against the live kernel's self entries)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusSample {
    pub rss_kb: Option<u64>,
    pub hwm_kb: Option<u64>,
    pub threads: Option<u64>,
}

pub fn parse_status(text: &str) -> Result<StatusSample> {
    let mut sample = StatusSample::default();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        match fields.next() {
            Some("VmRSS:") => {
                sample.rss_kb = Some(
                    fields
                        .next()
                        .context("VmRSS without a value")?
                        .parse()
                        .context("VmRSS is not numeric")?,
                )
            }
            Some("VmHWM:") => {
                sample.hwm_kb = Some(
                    fields
                        .next()
                        .context("VmHWM without a value")?
                        .parse()
                        .context("VmHWM is not numeric")?,
                )
            }
            Some("Threads:") => {
                sample.threads = Some(
                    fields
                        .next()
                        .context("Threads without a value")?
                        .parse()
                        .context("Threads is not numeric")?,
                )
            }
            _ => {}
        }
    }
    ensure!(
        sample.rss_kb.is_some() && sample.threads.is_some(),
        "status record missing VmRSS or Threads"
    );
    Ok(sample)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IoSample {
    pub read_bytes: Option<u64>,
    pub write_bytes: Option<u64>,
    /// `/proc/<pid>/io cancelled_write_bytes`: bytes this process accounted
    /// as written that were truncated away before writeback reached the
    /// device. Mandatory alongside the other two: it comes from the same
    /// file under the same permission, and without it `write_bytes` cannot
    /// be read as disk traffic.
    pub cancelled_write_bytes: Option<u64>,
}

pub fn parse_io(text: &str) -> Result<IoSample> {
    let mut sample = IoSample::default();
    fn value_of(name: &str, value: Option<&str>) -> Result<u64> {
        value
            .with_context(|| format!("{name} without a value"))?
            .parse()
            .with_context(|| format!("{name} is not numeric"))
    }
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let key = fields.next();
        let value = fields.next();
        match key {
            Some("read_bytes:") => sample.read_bytes = Some(value_of("read_bytes", value)?),
            Some("write_bytes:") => sample.write_bytes = Some(value_of("write_bytes", value)?),
            Some("cancelled_write_bytes:") => {
                sample.cancelled_write_bytes = Some(value_of("cancelled_write_bytes", value)?)
            }
            _ => {}
        }
    }
    ensure!(
        sample.read_bytes.is_some()
            && sample.write_bytes.is_some()
            && sample.cancelled_write_bytes.is_some(),
        "io record missing read_bytes, write_bytes or cancelled_write_bytes"
    );
    Ok(sample)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcStat {
    ppid: u32,
    pgrp: u32,
}

/// Parse /proc/<pid>/stat after the final `)` (comm may contain spaces or
/// parentheses). Fields 4 (ppid) and 5 (pgrp) of the post-comm layout.
fn parse_stat(text: &str) -> Result<ProcStat> {
    let rest = text
        .rsplit_once(')')
        .map(|(_, rest)| rest)
        .context("stat without a comm field")?
        .trim_start();
    let fields = rest.split_whitespace().collect::<Vec<_>>();
    ensure!(fields.len() >= 4, "stat record too short");
    let ppid = fields[1].parse().context("stat ppid is not numeric")?;
    let pgrp = fields[2].parse().context("stat pgrp is not numeric")?;
    Ok(ProcStat { ppid, pgrp })
}

/// Scan every numeric /proc entry's ppid/pgrp.
fn scan_proc() -> BTreeMap<u32, ProcStat> {
    let mut table = BTreeMap::new();
    if let Ok(entries) = fs::read_dir("/proc") {
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            let Ok(text) = fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            if let Ok(stat) = parse_stat(&text) {
                table.insert(pid, stat);
            }
        }
    }
    table
}

/// The measured process group (see module doc): the anchor plus every
/// transitive ppid descendant — the subject process tree.
pub fn group_pids(anchor: u32) -> Vec<u32> {
    let table = scan_proc();
    if !table.contains_key(&anchor) {
        return Vec::new();
    }
    let mut group = vec![anchor];
    let mut frontier = vec![anchor];
    let mut seen = std::collections::BTreeSet::from([anchor]);
    while let Some(parent) = frontier.pop() {
        for (pid, stat) in &table {
            if stat.ppid == parent && seen.insert(*pid) {
                group.push(*pid);
                frontier.push(*pid);
            }
        }
    }
    group.sort_unstable();
    group
}

/// Sum the used survivor/eden/old columns of one `jstat -gc` report into
/// whole KiB. jstat prints these as decimal KiB, so the columns are parsed as
/// f64 and rounded; a collector whose report lacks any of the four columns
/// (ZGC/Shenandoah name theirs differently) yields None, which the caller
/// records as a named heap gap rather than a zero.
pub fn parse_jstat_gc(text: &str) -> Option<u64> {
    let mut lines = text.lines();
    let columns = lines.next()?.split_whitespace().collect::<Vec<_>>();
    let numbers = lines.next()?.split_whitespace().collect::<Vec<_>>();
    let mut heap_kb = 0f64;
    // G1/parallel/serial all report the survivor/eden/old used columns.
    for wanted in ["S0U", "S1U", "EU", "OU"] {
        let index = columns.iter().position(|column| *column == wanted)?;
        let value = numbers.get(index)?.parse::<f64>().ok()?;
        if !value.is_finite() || value < 0.0 {
            return None;
        }
        heap_kb += value;
    }
    Some(heap_kb.round() as u64)
}

/// Java heap scope via `jstat -gc <pid> 1 1` (S0U+S1U+EU+OU, KiB). Returns
/// None when no jstat ships on PATH or the probe fails: the heap scope then
/// degrades to a named gap — RSS is still collected.
pub fn java_heap_kb(pid: u32) -> Option<u64> {
    let jstat = tool_on_path("jstat")?;
    let output = std::process::Command::new(jstat)
        .args(["-gc", &pid.to_string(), "1", "1"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_jstat_gc(&String::from_utf8_lossy(&output.stdout))
}

/// True when the executable named by argv[0] of /proc/<pid>/cmdline is a JVM
/// launcher (heap-scope candidate). Only argv[0] is inspected, so a fixture
/// path that merely contains "java" never misclassifies a native process.
pub fn is_java_pid(pid: u32) -> bool {
    fs::read(format!("/proc/{pid}/cmdline"))
        .ok()
        .and_then(|bytes| {
            let argv0 = bytes.split(|byte| *byte == 0).next()?;
            let argv0 = String::from_utf8_lossy(argv0).into_owned();
            Some(Path::new(&argv0).file_name()?.to_string_lossy() == "java")
        })
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Process sampling
// ---------------------------------------------------------------------------

/// One line of resources.tsv: one pid at one sample tick. Absent scopes are
/// `-`, never 0; an error note names the failed scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSample {
    pub seq: u64,
    pub elapsed_ms: u64,
    pub pid: u32,
    pub ppid: Option<u32>,
    pub pgrp: Option<u32>,
    pub rss_kb: Option<u64>,
    pub hwm_kb: Option<u64>,
    pub threads: Option<u64>,
    pub fds: Option<u64>,
    pub io_read_bytes: Option<u64>,
    pub io_write_bytes: Option<u64>,
    /// v2 column: `/proc/<pid>/io cancelled_write_bytes`.
    pub io_cancelled_write_bytes: Option<u64>,
    pub heap_kb: Option<u64>,
    pub note: String,
}

impl ProcessSample {
    fn line(&self) -> String {
        fn field(value: Option<u64>) -> String {
            value.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
        }
        fn pid_field(value: Option<u32>) -> String {
            value.map(|v| v.to_string()).unwrap_or_else(|| "-".into())
        }
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            self.seq,
            self.elapsed_ms,
            self.pid,
            pid_field(self.ppid),
            pid_field(self.pgrp),
            field(self.rss_kb),
            field(self.hwm_kb),
            field(self.threads),
            field(self.fds),
            field(self.io_read_bytes),
            field(self.io_write_bytes),
            field(self.io_cancelled_write_bytes),
            field(self.heap_kb),
            if self.note.is_empty() {
                "-"
            } else {
                &self.note
            },
        )
    }
}

/// Sample one pid once. `want_heap` enables the jstat heap probe for JVM
/// pids. A failed mandatory read becomes an `error:` note (dead-collector
/// evidence), never a silent zero.
fn sample_pid(pid: u32, want_heap: bool) -> Result<ProcessSample> {
    let base = format!("/proc/{pid}");
    let stat = fs::read_to_string(format!("{base}/stat"))
        .with_context(|| format!("read stat of pid {pid}"))?;
    let stat = parse_stat(&stat)?;
    let mut note = String::new();
    let mut push_note = |text: &str| {
        if !note.is_empty() {
            note.push(';');
        }
        note.push_str(text);
    };
    let status = match fs::read_to_string(format!("{base}/status")) {
        Ok(text) => match parse_status(&text) {
            Ok(sample) => Some(sample),
            Err(error) => {
                push_note(&format!("error:status:{error:#}"));
                None
            }
        },
        Err(error) => {
            push_note(&format!("error:status:{error}"));
            None
        }
    };
    let io = match fs::read_to_string(format!("{base}/io")) {
        Ok(text) => match parse_io(&text) {
            Ok(sample) => Some(sample),
            Err(error) => {
                push_note(&format!("error:io:{error:#}"));
                None
            }
        },
        Err(error) => {
            push_note(&format!("error:io:{error}"));
            None
        }
    };
    let fds = match fs::read_dir(format!("{base}/fd")) {
        Ok(entries) => Some(entries.flatten().count() as u64),
        Err(error) => {
            push_note(&format!("error:fd:{error}"));
            None
        }
    };
    let heap = if want_heap && is_java_pid(pid) {
        match java_heap_kb(pid) {
            Some(heap) => {
                push_note("heap:jstat");
                Some(heap)
            }
            None => {
                push_note("gap:heap:jstat-unavailable");
                None
            }
        }
    } else {
        None
    };
    Ok(ProcessSample {
        seq: 0,
        elapsed_ms: 0,
        pid,
        ppid: Some(stat.ppid),
        pgrp: Some(stat.pgrp),
        rss_kb: status.as_ref().and_then(|s| s.rss_kb),
        hwm_kb: status.as_ref().and_then(|s| s.hwm_kb),
        threads: status.as_ref().and_then(|s| s.threads),
        fds,
        io_read_bytes: io.as_ref().and_then(|s| s.read_bytes),
        io_write_bytes: io.as_ref().and_then(|s| s.write_bytes),
        io_cancelled_write_bytes: io.as_ref().and_then(|s| s.cancelled_write_bytes),
        heap_kb: heap,
        note,
    })
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SamplingSummary {
    pub ticks: u64,
    pub lines: u64,
    pub error_lines: u64,
}

/// Periodic process-group sampler. The writer owns a BufWriter over the
/// resources.tsv artifact; every tick writes complete LF-terminated lines and
/// flushes, so the validating reader can prove truncation.
pub struct ProcessCollector {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<SamplingSummary>>>,
}

impl ProcessCollector {
    pub fn start(anchor: u32, interval: Duration, file: &Path) -> Result<Self> {
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(file)
                .with_context(|| format!("create resources file {}", file.display()))?,
        );
        writeln!(
            writer,
            "{PROCESS_HEADER}\tinterval_ms={}",
            interval.as_millis()
        )?;
        writer.flush()?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let summary_thread = Arc::new(Mutex::new(SamplingSummary::default()));
        let file = file.to_path_buf();
        // Heap probes run every `heap_every` ticks (see HEAP_SAMPLE_INTERVAL).
        let heap_every = (HEAP_SAMPLE_INTERVAL.as_millis() / interval.as_millis().max(1)).max(1);
        let handle = std::thread::Builder::new()
            .name("resource-collector".into())
            .spawn(move || -> Result<SamplingSummary> {
                let mut seq = 0u64;
                let mut cumulative = SamplingSummary::default();
                let start = Instant::now();
                while !stop_thread.load(Ordering::Relaxed) {
                    let tick_start = Instant::now();
                    seq += 1;
                    let pids = group_pids(anchor);
                    let want_heap = u128::from(seq).is_multiple_of(heap_every);
                    let mut tick_summary = SamplingSummary::default();
                    {
                        let mut writer = BufWriter::new(
                            OpenOptions::new()
                                .append(true)
                                .open(&file)
                                .context("append resources sample")?,
                        );
                        for pid in pids {
                            if !Path::new(&format!("/proc/{pid}")).exists() {
                                // Vanished between the group discovery scan
                                // and this tick: not a dead collector.
                                continue;
                            }
                            match sample_pid(pid, want_heap) {
                                Ok(mut sample) => {
                                    if sample.note.contains("error:")
                                        && !Path::new(&format!("/proc/{pid}")).exists()
                                    {
                                        // Died mid-sample: partial record
                                        // dropped, not a dead collector.
                                        continue;
                                    }
                                    sample.seq = seq;
                                    sample.elapsed_ms = start.elapsed().as_millis() as u64;
                                    if sample.note.contains("error:") {
                                        tick_summary.error_lines += 1;
                                    }
                                    writer
                                        .write_all(sample.line().as_bytes())
                                        .context("write resources sample")?;
                                    tick_summary.lines += 1;
                                }
                                Err(error) => {
                                    tick_summary.error_lines += 1;
                                    writeln!(
                                        writer,
                                        "{seq}\t{}\t{pid}\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\terror:sample:{error:#}",
                                        start.elapsed().as_millis()
                                    )?;
                                }
                            }
                        }
                        writer.flush().context("flush resources samples")?;
                    }
                    cumulative.lines += tick_summary.lines;
                    cumulative.error_lines += tick_summary.error_lines;
                    cumulative.ticks = seq;
                    *summary_thread.lock().map_err(|_| {
                        anyhow::anyhow!("resource summary lock poisoned")
                    })? = cumulative;
                    let elapsed = tick_start.elapsed();
                    if elapsed < interval {
                        std::thread::sleep(interval - elapsed);
                    }
                }
                Ok(cumulative)
            })
            .context("spawn resource collector")?;
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }

    /// Stop sampling and join the collector. The returned summary counts
    /// every tick, line, and error line written.
    pub fn stop(mut self) -> Result<SamplingSummary> {
        self.stop.store(true, Ordering::Relaxed);
        let handle = self.handle.take().context("collector already stopped")?;
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("resource collector thread panicked"))?
    }
}

// ---------------------------------------------------------------------------
// Store sampling (fixture-root file lengths + allocated blocks)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreRecord {
    pub checkpoint: String,
    pub path: String,
    pub len: u64,
    pub allocated_bytes: u64,
}

/// Append one checkpoint sample over every file under `roots` to `file`
/// (created with the store header on first use). File lengths and allocated
/// blocks (st_blocks*512) are SEPARATE scopes recorded per file. The walk is
/// read-only; it never mutates the store.
pub fn sample_store(checkpoint: &str, roots: &[&Path], file: &Path) -> Result<usize> {
    let existed = file.exists();
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(file)
            .with_context(|| format!("open store sample file {}", file.display()))?,
    );
    if !existed {
        writeln!(writer, "{STORE_HEADER}")?;
    }
    let mut count = 0;
    fn walk(
        checkpoint: &str,
        directory: &Path,
        writer: &mut BufWriter<File>,
        count: &mut usize,
    ) -> Result<()> {
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("read store dir {}", directory.display()));
            }
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                walk(checkpoint, &path, writer, count)?;
            } else {
                writeln!(
                    writer,
                    "{}\t{}\t{}\t{}",
                    checkpoint,
                    path.display(),
                    metadata.len(),
                    metadata.blocks() * 512
                )?;
                *count += 1;
            }
        }
        Ok(())
    }
    for root in roots {
        if root.is_file() {
            let metadata = fs::metadata(root)
                .with_context(|| format!("stat store file {}", root.display()))?;
            writeln!(
                writer,
                "{}\t{}\t{}\t{}",
                checkpoint,
                root.display(),
                metadata.len(),
                metadata.blocks() * 512
            )?;
            count += 1;
            continue;
        }
        walk(checkpoint, root, &mut writer, &mut count)?;
    }
    writer.flush()?;
    Ok(count)
}

// ---------------------------------------------------------------------------
// Validating readers
// ---------------------------------------------------------------------------

fn torn_guard(text: &str, path: &Path) -> Result<()> {
    ensure!(
        text.is_empty() || text.ends_with('\n'),
        "{}: torn final line (truncated record)",
        path.display()
    );
    Ok(())
}

/// A pid-shaped column: `-` when the collector could not read the record at
/// all (a dead-collector line), otherwise a plain pid.
fn pid_field(path: &Path, field: &str) -> Result<Option<u32>> {
    if field == "-" {
        return Ok(None);
    }
    let value = field
        .parse::<u32>()
        .with_context(|| format!("{}: pid column is not a pid: {field:?}", path.display()))?;
    Ok(Some(value))
}

fn metric_field(path: &Path, field: &str) -> Result<Option<u64>> {
    if field == "-" {
        return Ok(None);
    }
    let value = field.parse::<u64>().with_context(|| {
        format!(
            "{}: metric is not a non-negative integer: {field:?}",
            path.display()
        )
    })?;
    Ok(Some(value))
}

/// Read and validate a resources.tsv artifact. Rejects a missing/torn final
/// line, a header that is not this schema version, a wrong field count, and
/// non-numeric metrics. Error-note records are returned (the ROW decides:
/// mandatory-scope errors fail the row).
pub fn read_process_samples(path: &Path) -> Result<Vec<ProcessSample>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read resources file {}", path.display()))?;
    torn_guard(&text, path)?;
    let mut samples = Vec::new();
    let mut last_seq_by_pid = BTreeMap::new();
    for line in text.lines() {
        if line.starts_with('#') {
            // A v1 artifact has one fewer column; reading it with v2 offsets
            // would silently misreport heap as cancelled writes.
            ensure!(
                line.split('\t')
                    .next()
                    .is_some_and(|version| version == PROCESS_HEADER),
                "{}: resources header is not {PROCESS_HEADER}: {line:?}",
                path.display()
            );
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        ensure!(
            fields.len() == PROCESS_FIELDS,
            "{}: resources record must have {PROCESS_FIELDS} fields, got {}: {line:?}",
            path.display(),
            fields.len()
        );
        let seq = fields[0]
            .parse::<u64>()
            .with_context(|| format!("{}: sample seq is not numeric", path.display()))?;
        let pid = fields[2]
            .parse::<u32>()
            .with_context(|| format!("{}: pid is not numeric", path.display()))?;
        if let Some(last) = last_seq_by_pid.get(&pid) {
            ensure!(
                seq > *last,
                "{}: sample seq {seq} for pid {pid} is not increasing (last {last})",
                path.display()
            );
        }
        last_seq_by_pid.insert(pid, seq);
        let note = fields[13];
        samples.push(ProcessSample {
            seq,
            elapsed_ms: fields[1]
                .parse()
                .with_context(|| format!("{}: elapsed_ms is not numeric", path.display()))?,
            pid: fields[2]
                .parse()
                .with_context(|| format!("{}: pid is not numeric", path.display()))?,
            ppid: pid_field(path, fields[3])?,
            pgrp: pid_field(path, fields[4])?,
            rss_kb: metric_field(path, fields[5])?,
            hwm_kb: metric_field(path, fields[6])?,
            threads: metric_field(path, fields[7])?,
            fds: metric_field(path, fields[8])?,
            io_read_bytes: metric_field(path, fields[9])?,
            io_write_bytes: metric_field(path, fields[10])?,
            io_cancelled_write_bytes: metric_field(path, fields[11])?,
            heap_kb: metric_field(path, fields[12])?,
            note: if note == "-" {
                String::new()
            } else {
                note.into()
            },
        });
    }
    Ok(samples)
}

/// One byte-counter's movement for one pid across its sampled window:
/// (delta_bytes, span_ms, bytes_per_second). `None` when the scope was never
/// collected for that pid, when fewer than two samples carry it, or when the
/// span is zero — an unmeasurable rate is absent, never reported as 0.
pub fn counter_rate(
    samples: &[ProcessSample],
    pid: u32,
    scope: fn(&ProcessSample) -> Option<u64>,
) -> Option<(u64, u64, u64)> {
    let series: Vec<(u64, u64)> = samples
        .iter()
        .filter(|sample| sample.pid == pid)
        .filter_map(|sample| scope(sample).map(|value| (sample.elapsed_ms, value)))
        .collect();
    let (first_ms, first) = *series.first()?;
    let (last_ms, last) = *series.last()?;
    if series.len() < 2 {
        return None;
    }
    let span = last_ms.checked_sub(first_ms).filter(|span| *span > 0)?;
    let delta = last.saturating_sub(first);
    Some((delta, span, delta.saturating_mul(1000) / span))
}

// ---------------------------------------------------------------------------
// Network-byte scope (milestone 18d)
// ---------------------------------------------------------------------------

/// `network.tsv` schema. Separate from the process schema on purpose: network
/// bytes are their own scope and are never mixed into a resources record.
pub const NETWORK_HEADER: &str = "# pipestream-network-v1";
/// Column count of one network.tsv record.
pub const NETWORK_FIELDS: usize = 8;

/// One interface's cumulative counters at one instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterfaceCounters {
    pub interface: String,
    pub rx_bytes: u64,
    pub rx_packets: u64,
    pub tx_bytes: u64,
    pub tx_packets: u64,
}

/// Parse `/proc/net/dev` into per-interface counters.
///
/// The header is two lines; every later line is `iface: rx_bytes rx_packets
/// rx_errs rx_drop rx_fifo rx_frame rx_compressed rx_multicast tx_bytes
/// tx_packets ...`. An interface name may abut its colon, so the split is on
/// the colon rather than on whitespace.
pub fn parse_net_dev(text: &str) -> Result<Vec<InterfaceCounters>> {
    let mut interfaces = Vec::new();
    for line in text.lines().skip(2) {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        let fields = rest.split_whitespace().collect::<Vec<_>>();
        ensure!(
            fields.len() >= 10,
            "/proc/net/dev record for {} has {} fields, expected at least 10",
            name.trim(),
            fields.len()
        );
        let number = |index: usize, what: &str| -> Result<u64> {
            fields[index].parse::<u64>().with_context(|| {
                format!("/proc/net/dev {what} is not numeric: {:?}", fields[index])
            })
        };
        interfaces.push(InterfaceCounters {
            interface: name.trim().to_owned(),
            rx_bytes: number(0, "rx_bytes")?,
            rx_packets: number(1, "rx_packets")?,
            tx_bytes: number(8, "tx_bytes")?,
            tx_packets: number(9, "tx_packets")?,
        });
    }
    ensure!(!interfaces.is_empty(), "/proc/net/dev listed no interfaces");
    Ok(interfaces)
}

/// Read one interface's counters from a pid's network namespace view.
///
/// The path is `/proc/<pid>/net/dev`, which reports the NAMESPACE the pid
/// belongs to. On a host that grants no network namespace this is the same
/// namespace as the driver's, which is exactly why a row using it must state
/// that its scope is the host's loopback and not the fixture's.
pub fn interface_counters(pid: u32, interface: &str) -> Result<InterfaceCounters> {
    let path = format!("/proc/{pid}/net/dev");
    let text =
        fs::read_to_string(&path).with_context(|| format!("read interface counters {path}"))?;
    parse_net_dev(&text)?
        .into_iter()
        .find(|counters| counters.interface == interface)
        .with_context(|| format!("{path} has no interface {interface}"))
}

/// One network.tsv record: one interface sample at one named checkpoint, with
/// the collection method on the line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkSample {
    pub checkpoint: String,
    pub method: String,
    pub interface: String,
    pub elapsed_ms: u64,
    pub rx_bytes: u64,
    pub rx_packets: u64,
    pub tx_bytes: u64,
    pub tx_packets: u64,
}

impl NetworkSample {
    fn line(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            self.checkpoint,
            self.method,
            self.interface,
            self.elapsed_ms,
            self.rx_bytes,
            self.rx_packets,
            self.tx_bytes,
            self.tx_packets
        )
    }
}

/// Append one network sample, creating the file with its header on first use.
pub fn append_network_sample(file: &Path, sample: &NetworkSample) -> Result<()> {
    let existed = file.exists();
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(file)
            .with_context(|| format!("open network sample file {}", file.display()))?,
    );
    if !existed {
        writeln!(writer, "{NETWORK_HEADER}")?;
    }
    writer.write_all(sample.line().as_bytes())?;
    writer.flush()?;
    Ok(())
}

/// Read and validate a network.tsv artifact. Same rules as the process
/// reader: a torn final line, a wrong header, a wrong field count or a
/// non-numeric counter is a rejection, so a truncated record can never pass
/// silently as a smaller measurement.
pub fn read_network_samples(path: &Path) -> Result<Vec<NetworkSample>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read network file {}", path.display()))?;
    torn_guard(&text, path)?;
    let mut samples = Vec::new();
    for line in text.lines() {
        if line.starts_with('#') {
            ensure!(
                line == NETWORK_HEADER,
                "{}: network header is not {NETWORK_HEADER}: {line:?}",
                path.display()
            );
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        ensure!(
            fields.len() == NETWORK_FIELDS,
            "{}: network record must have {NETWORK_FIELDS} fields, got {}: {line:?}",
            path.display(),
            fields.len()
        );
        ensure!(
            !fields[1].is_empty() && fields[1] != "-",
            "{}: network record carries no collection method: {line:?}",
            path.display()
        );
        let number = |index: usize| -> Result<u64> {
            fields[index].parse::<u64>().with_context(|| {
                format!(
                    "{}: network counter is not a non-negative integer: {:?}",
                    path.display(),
                    fields[index]
                )
            })
        };
        samples.push(NetworkSample {
            checkpoint: fields[0].to_owned(),
            method: fields[1].to_owned(),
            interface: fields[2].to_owned(),
            elapsed_ms: number(3)?,
            rx_bytes: number(4)?,
            rx_packets: number(5)?,
            tx_bytes: number(6)?,
            tx_packets: number(7)?,
        });
    }
    Ok(samples)
}

/// Nearest-rank percentile of an already sorted, non-empty series.
fn nearest_rank(sorted: &[u64], percent: u64) -> u64 {
    let index = (sorted.len() as u64 * percent)
        .div_ceil(100)
        .saturating_sub(1) as usize;
    sorted[index.min(sorted.len() - 1)]
}

/// Statistics of one measurement window over the WHOLE sampled process group.
///
/// Every figure is a per-tick group SUM first and a statistic second: the RSS
/// of a process group at one instant is the sum of its members' RSS at that
/// same tick, so summing per-pid statistics would mix ticks and invent a
/// number no instant ever had. `pids_min`/`pids_max` make the group size
/// visible, because a window whose group changed size is a different
/// measurement from one whose group did not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowStats {
    /// Sampling ticks inside the window (not sample lines).
    pub ticks: usize,
    pub pids_min: usize,
    pub pids_max: usize,
    pub rss_median_kb: u64,
    pub rss_p90_kb: u64,
    pub rss_max_kb: u64,
    pub hwm_max_kb: u64,
    pub threads_median: u64,
    pub threads_max: u64,
    pub fd_median: u64,
    pub fd_p90: u64,
    pub fd_max: u64,
    /// Ticks on which the Java heap scope was collected (jstat cadence).
    pub heap_ticks: usize,
    pub heap_min_kb: Option<u64>,
    pub heap_max_kb: Option<u64>,
    /// Ticks that recorded a named heap gap (`gap:heap:...`).
    pub heap_gap_ticks: usize,
}

/// Group window statistics over `[from_ms, to_ms]` (inclusive).
///
/// Fails when the window contains no tick: a measurement window with no
/// samples is an unmeasured window, and an unmeasured window is never
/// reported as zero.
pub fn group_window_stats(
    samples: &[ProcessSample],
    from_ms: u64,
    to_ms: u64,
    label: &str,
) -> Result<WindowStats> {
    struct Tick {
        elapsed_ms: u64,
        pids: usize,
        rss_kb: u64,
        hwm_kb: u64,
        threads: u64,
        fds: u64,
        heap_kb: Option<u64>,
        heap_gap: bool,
    }
    let mut ticks: BTreeMap<u64, Tick> = BTreeMap::new();
    for sample in samples {
        if sample.elapsed_ms < from_ms || sample.elapsed_ms > to_ms {
            continue;
        }
        let tick = ticks.entry(sample.seq).or_insert(Tick {
            elapsed_ms: sample.elapsed_ms,
            pids: 0,
            rss_kb: 0,
            hwm_kb: 0,
            threads: 0,
            fds: 0,
            heap_kb: None,
            heap_gap: false,
        });
        tick.pids += 1;
        tick.elapsed_ms = tick.elapsed_ms.min(sample.elapsed_ms);
        tick.rss_kb += sample.rss_kb.unwrap_or(0);
        tick.hwm_kb += sample.hwm_kb.unwrap_or(0);
        tick.threads += sample.threads.unwrap_or(0);
        tick.fds += sample.fds.unwrap_or(0);
        if let Some(heap) = sample.heap_kb {
            tick.heap_kb = Some(tick.heap_kb.unwrap_or(0) + heap);
        }
        if sample.note.contains("gap:heap") {
            tick.heap_gap = true;
        }
    }
    ensure!(
        !ticks.is_empty(),
        "window {label} [{from_ms}ms, {to_ms}ms] contains no sampling tick; an \
         unmeasured window is never reported as zero"
    );
    let mut rss: Vec<u64> = Vec::with_capacity(ticks.len());
    let mut threads: Vec<u64> = Vec::with_capacity(ticks.len());
    let mut fds: Vec<u64> = Vec::with_capacity(ticks.len());
    let mut heap: Vec<u64> = Vec::new();
    let mut hwm_max = 0;
    let mut heap_gap_ticks = 0;
    let mut pids_min = usize::MAX;
    let mut pids_max = 0;
    for tick in ticks.values() {
        rss.push(tick.rss_kb);
        threads.push(tick.threads);
        fds.push(tick.fds);
        hwm_max = hwm_max.max(tick.hwm_kb);
        if let Some(value) = tick.heap_kb {
            heap.push(value);
        }
        if tick.heap_gap {
            heap_gap_ticks += 1;
        }
        pids_min = pids_min.min(tick.pids);
        pids_max = pids_max.max(tick.pids);
    }
    let ticks_count = ticks.len();
    rss.sort_unstable();
    threads.sort_unstable();
    fds.sort_unstable();
    Ok(WindowStats {
        ticks: ticks_count,
        pids_min,
        pids_max,
        rss_median_kb: median(&rss),
        rss_p90_kb: nearest_rank(&rss, 90),
        rss_max_kb: *rss.last().expect("non-empty"),
        hwm_max_kb: hwm_max,
        threads_median: median(&threads),
        threads_max: *threads.last().expect("non-empty"),
        fd_median: median(&fds),
        fd_p90: nearest_rank(&fds, 90),
        fd_max: *fds.last().expect("non-empty"),
        heap_ticks: heap.len(),
        heap_min_kb: heap.iter().min().copied(),
        heap_max_kb: heap.iter().max().copied(),
        heap_gap_ticks,
    })
}

/// Median of a sorted, non-empty series.
fn median(sorted: &[u64]) -> u64 {
    let mid = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[mid - 1] + sorted[mid]) / 2
    } else {
        sorted[mid]
    }
}

/// Read and validate a store.tsv artifact (same truncation/field rules).
pub fn read_store_samples(path: &Path) -> Result<Vec<StoreRecord>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read store sample file {}", path.display()))?;
    torn_guard(&text, path)?;
    let mut records = Vec::new();
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        ensure!(
            fields.len() == 4,
            "{}: store record must have 4 fields, got {}: {line:?}",
            path.display(),
            fields.len()
        );
        records.push(StoreRecord {
            checkpoint: fields[0].to_owned(),
            path: fields[1].to_owned(),
            len: fields[2]
                .parse()
                .with_context(|| format!("{}: file length is not numeric", path.display()))?,
            allocated_bytes: fields[3]
                .parse()
                .with_context(|| format!("{}: allocated bytes is not numeric", path.display()))?,
        });
    }
    Ok(records)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_and_io_parsers_reject_truncation() {
        let status =
            parse_status("Name:\tt\nVmHWM:\t  1000 kB\nVmRSS:\t 2000 kB\nThreads:\t7\n").unwrap();
        assert_eq!(status.rss_kb, Some(2000));
        assert_eq!(status.hwm_kb, Some(1000));
        assert_eq!(status.threads, Some(7));
        assert!(
            parse_status("Threads:\t1\n").is_err(),
            "missing VmRSS is rejected"
        );
        let io = parse_io("read_bytes: 12\nwrite_bytes: 34\ncancelled_write_bytes: 8\n").unwrap();
        assert_eq!(io.read_bytes, Some(12));
        assert_eq!(io.write_bytes, Some(34));
        assert_eq!(io.cancelled_write_bytes, Some(8));
        assert!(
            parse_io("read_bytes: 12\n").is_err(),
            "partial io is rejected"
        );
        assert!(
            parse_io("read_bytes: 12\nwrite_bytes: 34\n").is_err(),
            "a v1-shaped io record without cancelled_write_bytes is rejected, \
             not defaulted to zero"
        );
        assert!(
            parse_io("read_bytes: 12\nwrite_bytes: 34\ncancelled_write_bytes: x\n").is_err(),
            "a non-numeric cancelled_write_bytes is rejected"
        );
    }

    #[test]
    fn stat_parser_takes_fields_after_the_last_paren() {
        let stat = parse_stat("123 (weird (name) S 1 2 3 4 5 6").unwrap();
        assert_eq!(stat.ppid, 1);
        assert_eq!(stat.pgrp, 2);
        assert!(parse_stat("no parens").is_err());
    }

    #[test]
    fn group_discovery_walks_the_anchor_and_its_children() {
        let self_pid = std::process::id();
        let group = group_pids(self_pid);
        assert!(group.contains(&self_pid), "anchor is in its own group");
        // A direct child is discovered by the parent-pid walk.
        let mut child = std::process::Command::new("sleep")
            .arg("0.2")
            .spawn()
            .unwrap();
        let child_pid = child.id();
        let group = group_pids(self_pid);
        assert!(
            group.contains(&child_pid),
            "live child (same pgrp) must be in the group"
        );
        child.wait().unwrap();
    }

    #[test]
    fn sampling_self_reports_rss_threads_fds_and_io() {
        let self_pid = std::process::id();
        let sample = sample_pid(self_pid, false).unwrap();
        assert!(sample.rss_kb.unwrap() > 0, "RSS must never be zero-valued");
        assert!(sample.threads.unwrap() > 0);
        assert!(sample.fds.unwrap() > 0);
        assert!(sample.io_read_bytes.is_some(), "self io must be readable");
        assert!(
            sample.io_cancelled_write_bytes.is_some(),
            "self cancelled_write_bytes must be readable"
        );
        assert!(sample.note.is_empty(), "self sample has no error notes");
    }

    #[test]
    fn calibration_reports_positive_overhead() {
        let self_pid = std::process::id();
        let calibration = calibrate(self_pid, 5).unwrap();
        assert_eq!(calibration.samples, 5);
        assert!(calibration.per_sample_ns > 0);
    }

    #[test]
    fn host_facts_describe_this_machine_and_fs() {
        let directory = tempfile::tempdir().unwrap();
        let facts = host_facts(directory.path()).unwrap();
        assert!(!facts.os_type.is_empty());
        assert!(facts.cpu_count >= 1);
        assert!(facts.mem_total_kb > 0);
        assert!(!facts.fixture_fs_type.is_empty());
        assert!(!facts.fixture_mount.is_empty());
        assert!(facts.fixture_mount.len() > 1, "a mount point was matched");
    }

    #[test]
    fn permissions_and_tool_probe_are_honest() {
        let permissions = proc_permissions(std::process::id());
        assert!(permissions.io, "self /proc/<pid>/io must be readable");
        assert!(permissions.status);
        assert!(permissions.fd);
        // tool_on_path must not invent a tool: a nonsense name is absent.
        assert!(tool_on_path("definitely-not-a-real-tool-pipestream").is_none());
    }

    #[test]
    fn store_sampling_records_lengths_and_blocks() {
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("store");
        fs::create_dir_all(store.join("nested")).unwrap();
        fs::write(store.join("a.bin"), vec![7u8; 100]).unwrap();
        fs::write(store.join("nested/b.bin"), vec![8u8; 4096]).unwrap();
        let file = directory.path().join("store.tsv");
        let count = sample_store("c0", &[&store], &file).unwrap();
        assert_eq!(count, 2);
        sample_store("c1", &[&store], &file).unwrap();
        let mut records = read_store_samples(&file).unwrap();
        assert_eq!(records.len(), 4);
        records.sort_by(|a, b| a.path.cmp(&b.path).then(a.checkpoint.cmp(&b.checkpoint)));
        assert_eq!(records[0].checkpoint, "c0");
        assert_eq!(records[0].len, 100);
        assert!(records[0].allocated_bytes >= 512, "blocks are allocated");
        assert_eq!(records[1].checkpoint, "c1");
        assert_eq!(records[1].len, 100);
        assert_eq!(records[3].len, 4096);
    }

    #[test]
    fn resources_reader_rejects_torn_and_malformed_records() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("resources.tsv");
        let good = format!(
            "{PROCESS_HEADER}\n1\t0\t{pid}\t1\t1\t100\t100\t1\t2\t0\t4096\t512\t-\t-\n",
            pid = std::process::id()
        );
        fs::write(&file, &good).unwrap();
        let samples = read_process_samples(&file).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].rss_kb, Some(100));
        assert_eq!(samples[0].io_write_bytes, Some(4096));
        assert_eq!(samples[0].io_cancelled_write_bytes, Some(512));
        assert_eq!(samples[0].heap_kb, None, "absent scope is -, not zero");
        // A v1 artifact (13 columns, v1 header) is refused outright rather
        // than read with v2 column offsets.
        fs::write(
            &file,
            format!(
                "# pipestream-resources-v1\n1\t0\t{pid}\t1\t1\t100\t100\t1\t2\t0\t4096\t-\t-\n",
                pid = std::process::id()
            ),
        )
        .unwrap();
        assert!(
            read_process_samples(&file).is_err(),
            "a v1 resources artifact is rejected by the v2 reader"
        );
        fs::write(&file, &good).unwrap();
        // Torn final line.
        let mut torn = good.clone();
        torn.pop();
        fs::write(&file, torn).unwrap();
        assert!(read_process_samples(&file).is_err(), "torn record rejected");
        // Wrong field count.
        fs::write(&file, format!("{PROCESS_HEADER}\n1\t0\t1\t1\n")).unwrap();
        assert!(read_process_samples(&file).is_err());
        // Non-numeric metric.
        fs::write(
            &file,
            format!(
                "{PROCESS_HEADER}\n1\t0\t{pid}\t1\t1\tlots\t-\t1\t2\t0\t0\t0\t-\t-\n",
                pid = std::process::id()
            ),
        )
        .unwrap();
        assert!(read_process_samples(&file).is_err());
    }

    #[test]
    fn java_pid_detection_reads_argv0_not_the_whole_command_line() {
        // This test process is not a JVM even though its command line and
        // working directory mention java fixture paths.
        assert!(!is_java_pid(std::process::id()));
        assert!(!is_java_pid(u32::MAX), "an absent pid is not a JVM");
    }

    #[test]
    fn jstat_gc_parser_sums_the_used_columns_as_whole_kib() {
        let report = "  S0C   S1C   S0U      S1U       EC        EU     OC        OU\n                       0.0   16384.0   0.0   12079.6   16384.0   0.0   49152.0   4088.8\n";
        // 12079.6 + 4088.8 = 16168.4 KiB used across survivors, eden and old.
        assert_eq!(parse_jstat_gc(report), Some(16168));
        // A collector without the classic generation columns is a named gap,
        // never a zero.
        assert_eq!(parse_jstat_gc("ZGC0   ZGC1\n1.0   2.0\n"), None);
        assert_eq!(
            parse_jstat_gc("S0U   S1U   EU   OU\n0.0   -   0.0   1.0\n"),
            None,
            "a non-numeric column is a gap, not a partial sum"
        );
        assert_eq!(parse_jstat_gc("S0U   S1U   EU   OU\n"), None);
    }

    #[test]
    fn dead_collector_line_is_read_back_and_surfaced() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("resources.tsv");
        // Exactly the shape the collector writes when a whole sample fails:
        // pid columns are '-' because nothing could be read for that pid.
        fs::write(
            &file,
            format!(
                "{PROCESS_HEADER}\n7\t700\t4242\t-\t-\t-\t-\t-\t-\t-\t-\t-\t-\terror:sample:read stat of pid 4242\n"
            ),
        )
        .unwrap();
        let samples = read_process_samples(&file).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].ppid, None);
        assert_eq!(samples[0].rss_kb, None, "a dead sample is absent, not zero");
        assert_eq!(
            samples[0].io_cancelled_write_bytes, None,
            "the v2 column is absent on a dead sample, not zero"
        );
        assert!(
            samples[0].note.contains("error:"),
            "the dead-collector note survives the reader so the row can fail on it"
        );
    }

    #[test]
    fn counter_rate_is_absent_when_it_cannot_be_measured() {
        let sample = |elapsed_ms: u64, write: Option<u64>, cancelled: Option<u64>| ProcessSample {
            seq: elapsed_ms / 100,
            elapsed_ms,
            pid: 42,
            ppid: Some(1),
            pgrp: Some(1),
            rss_kb: Some(1),
            hwm_kb: Some(1),
            threads: Some(1),
            fds: Some(1),
            io_read_bytes: Some(0),
            io_write_bytes: write,
            io_cancelled_write_bytes: cancelled,
            heap_kb: None,
            note: String::new(),
        };
        let series = vec![
            sample(0, Some(1_000), Some(500)),
            sample(1_000, Some(3_000), Some(2_500)),
            sample(2_000, Some(5_000), None),
        ];
        assert_eq!(
            counter_rate(&series, 42, |sample| sample.io_write_bytes),
            Some((4_000, 2_000, 2_000))
        );
        // The cancelled scope has only two collected points, one second apart.
        assert_eq!(
            counter_rate(&series, 42, |sample| sample.io_cancelled_write_bytes),
            Some((2_000, 1_000, 2_000))
        );
        // A pid that was never sampled has no rate at all.
        assert_eq!(
            counter_rate(&series, 7, |sample| sample.io_write_bytes),
            None
        );
        // One point, or a zero span, is unmeasurable — absent, not zero.
        assert_eq!(
            counter_rate(&series[..1], 42, |sample| sample.io_write_bytes),
            None
        );
        let instant = vec![sample(5, Some(1), None), sample(5, Some(9), None)];
        assert_eq!(
            counter_rate(&instant, 42, |sample| sample.io_write_bytes),
            None
        );
    }

    #[test]
    fn store_reader_rejects_torn_records() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("store.tsv");
        fs::write(&file, format!("{STORE_HEADER}\nc0\t/a\t1\t512\n")).unwrap();
        assert_eq!(read_store_samples(&file).unwrap().len(), 1);
        fs::write(&file, format!("{STORE_HEADER}\nc0\t/a\t1\t512")).unwrap();
        assert!(
            read_store_samples(&file).is_err(),
            "torn store record rejected"
        );
    }

    #[test]
    fn collector_samples_a_live_process_group() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("resources.tsv");
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap();
        let anchor = child.id();
        let collector = ProcessCollector::start(anchor, Duration::from_millis(20), &file).unwrap();
        std::thread::sleep(Duration::from_millis(120));
        let summary = collector.stop().unwrap();
        assert!(summary.ticks >= 2, "collector ticked at least twice");
        let samples = read_process_samples(&file).unwrap();
        assert!(samples.len() as u64 >= summary.lines);
        assert_eq!(summary.error_lines, 0, "no dead-collector notes");
        assert!(
            samples
                .iter()
                .all(|sample| sample.io_cancelled_write_bytes.is_some()),
            "the v2 cancelled-write scope is collected on every live sample"
        );
        assert!(
            samples.iter().all(|sample| sample.pid == anchor),
            "only the anchored subject tree is sampled"
        );
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(
            samples.iter().all(|sample| sample.seq >= 1),
            "every record carries its tick sequence"
        );
    }

    fn sample(
        seq: u64,
        elapsed_ms: u64,
        pid: u32,
        rss_kb: u64,
        heap_kb: Option<u64>,
    ) -> ProcessSample {
        ProcessSample {
            seq,
            elapsed_ms,
            pid,
            ppid: Some(1),
            pgrp: Some(1),
            rss_kb: Some(rss_kb),
            hwm_kb: Some(rss_kb + 10),
            threads: Some(4),
            fds: Some(7),
            io_read_bytes: Some(0),
            io_write_bytes: Some(0),
            io_cancelled_write_bytes: Some(0),
            heap_kb,
            note: if heap_kb.is_some() {
                "heap:jstat".into()
            } else {
                String::new()
            },
        }
    }

    #[test]
    fn group_window_stats_sums_each_tick_before_taking_statistics() {
        // Two pids per tick: the group RSS at a tick is their SUM, so the
        // window median must be a sum that an instant really had (300, 400,
        // 500), never a sum of per-pid medians.
        let samples = vec![
            sample(1, 0, 10, 100, None),
            sample(1, 0, 11, 200, None),
            sample(2, 100, 10, 150, Some(64)),
            sample(2, 100, 11, 250, None),
            sample(3, 200, 10, 200, None),
            sample(3, 200, 11, 300, None),
        ];
        let stats = group_window_stats(&samples, 0, 200, "all").unwrap();
        assert_eq!(stats.ticks, 3);
        assert_eq!(stats.pids_min, 2);
        assert_eq!(stats.pids_max, 2);
        assert_eq!(stats.rss_median_kb, 400);
        assert_eq!(stats.rss_max_kb, 500);
        assert_eq!(stats.hwm_max_kb, 520);
        assert_eq!(stats.threads_max, 8);
        assert_eq!(stats.fd_max, 14);
        assert_eq!(stats.heap_ticks, 1);
        assert_eq!(stats.heap_min_kb, Some(64));
        assert_eq!(stats.heap_gap_ticks, 0);
        // A sub-window selects only the ticks inside it.
        let tail = group_window_stats(&samples, 200, 200, "tail").unwrap();
        assert_eq!(tail.ticks, 1);
        assert_eq!(tail.rss_median_kb, 500);
        // An empty window is a measurement failure, never a zero row.
        let error = group_window_stats(&samples, 900, 1000, "gap").unwrap_err();
        assert!(
            format!("{error:#}").contains("no sampling tick"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn net_dev_parser_reads_the_live_kernel_and_rejects_short_records() {
        let text = fs::read_to_string("/proc/net/dev").unwrap();
        let interfaces = parse_net_dev(&text).unwrap();
        assert!(
            interfaces.iter().any(|i| i.interface == "lo"),
            "the live kernel lists a loopback interface"
        );
        // Column positions matter: rx is fields 0/1 and tx is fields 8/9.
        let sample = parse_net_dev(
            "Inter-|   Receive  |  Transmit\n \
             face |bytes packets errs drop fifo frame compressed multicast|bytes packets\n\
             \x20   lo: 11 22 0 0 0 0 0 0 33 44 0 0 0 0 0 0\n",
        )
        .unwrap();
        assert_eq!(sample.len(), 1);
        assert_eq!(sample[0].interface, "lo");
        assert_eq!(sample[0].rx_bytes, 11);
        assert_eq!(sample[0].rx_packets, 22);
        assert_eq!(sample[0].tx_bytes, 33);
        assert_eq!(sample[0].tx_packets, 44);
        assert!(
            parse_net_dev("a\nb\n   lo: 1 2 3\n").is_err(),
            "a short record is rejected, not read with shifted offsets"
        );
        assert!(
            parse_net_dev("a\nb\n").is_err(),
            "an interface-free file is rejected rather than read as zero traffic"
        );
    }

    #[test]
    fn network_reader_rejects_truncation_and_a_missing_method() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("network.tsv");
        let sample = NetworkSample {
            checkpoint: "baseline".into(),
            method: "proc-net-dev".into(),
            interface: "lo".into(),
            elapsed_ms: 10,
            rx_bytes: 1,
            rx_packets: 2,
            tx_bytes: 3,
            tx_packets: 4,
        };
        append_network_sample(&file, &sample).unwrap();
        append_network_sample(&file, &sample).unwrap();
        assert_eq!(read_network_samples(&file).unwrap().len(), 2);
        // A dead collector that stopped mid-record must not read back as a
        // smaller measurement.
        let text = fs::read_to_string(&file).unwrap();
        fs::write(&file, &text[..text.len() - 6]).unwrap();
        let error = read_network_samples(&file).unwrap_err();
        assert!(
            format!("{error:#}").contains("torn final line"),
            "unexpected error: {error:#}"
        );
        // A record without a stated collection method is rejected: the
        // method is recorded per sample and never inferred.
        fs::write(
            &file,
            format!("{NETWORK_HEADER}\nbaseline\t-\tlo\t1\t2\t3\t4\t5\n"),
        )
        .unwrap();
        assert!(read_network_samples(&file).is_err());
        // A wrong header version is rejected outright.
        fs::write(
            &file,
            "# pipestream-network-v0\nbaseline\tm\tlo\t1\t2\t3\t4\t5\n",
        )
        .unwrap();
        assert!(read_network_samples(&file).is_err());
    }

    #[test]
    fn nearest_rank_percentile_never_leaves_the_series() {
        assert_eq!(nearest_rank(&[1], 90), 1);
        assert_eq!(nearest_rank(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 90), 9);
        assert_eq!(nearest_rank(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 100), 10);
        assert_eq!(nearest_rank(&[1, 2, 3], 50), 2);
        assert_eq!(median(&[1, 2, 3, 4]), 2);
        assert_eq!(median(&[1, 2, 3]), 2);
    }
}
