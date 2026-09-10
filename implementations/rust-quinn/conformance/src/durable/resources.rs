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

pub const PROCESS_HEADER: &str = "# pipestream-resources-v1";
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
}

pub fn parse_io(text: &str) -> Result<IoSample> {
    let mut sample = IoSample::default();
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        match fields.next() {
            Some("read_bytes:") => {
                sample.read_bytes = Some(
                    fields
                        .next()
                        .context("read_bytes without a value")?
                        .parse()
                        .context("read_bytes is not numeric")?,
                )
            }
            Some("write_bytes:") => {
                sample.write_bytes = Some(
                    fields
                        .next()
                        .context("write_bytes without a value")?
                        .parse()
                        .context("write_bytes is not numeric")?,
                )
            }
            _ => {}
        }
    }
    ensure!(
        sample.read_bytes.is_some() && sample.write_bytes.is_some(),
        "io record missing read_bytes or write_bytes"
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
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
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
                                        "{seq}\t{}\t{pid}\t-\t-\t-\t-\t-\t-\t-\t-\t-\terror:sample:{error:#}",
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
/// line, a wrong field count, and non-numeric metrics. Error-note records are
/// returned (the ROW decides: mandatory-scope errors fail the row).
pub fn read_process_samples(path: &Path) -> Result<Vec<ProcessSample>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read resources file {}", path.display()))?;
    torn_guard(&text, path)?;
    let mut samples = Vec::new();
    let mut last_seq_by_pid = BTreeMap::new();
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        ensure!(
            fields.len() == 13,
            "{}: resources record must have 13 fields, got {}: {line:?}",
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
        let note = fields[12];
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
            heap_kb: metric_field(path, fields[11])?,
            note: if note == "-" {
                String::new()
            } else {
                note.into()
            },
        });
    }
    Ok(samples)
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
        let io = parse_io("read_bytes: 12\nwrite_bytes: 34\n").unwrap();
        assert_eq!(io.read_bytes, Some(12));
        assert_eq!(io.write_bytes, Some(34));
        assert!(
            parse_io("read_bytes: 12\n").is_err(),
            "partial io is rejected"
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
            "{PROCESS_HEADER}\n1\t0\t{pid}\t1\t1\t100\t100\t1\t2\t0\t0\t-\t-\n",
            pid = std::process::id()
        );
        fs::write(&file, &good).unwrap();
        let samples = read_process_samples(&file).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].rss_kb, Some(100));
        assert_eq!(samples[0].heap_kb, None, "absent scope is -, not zero");
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
                "{PROCESS_HEADER}\n1\t0\t{pid}\t1\t1\tlots\t-\t1\t2\t0\t0\t-\t-\n",
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
                "{PROCESS_HEADER}\n7\t700\t4242\t-\t-\t-\t-\t-\t-\t-\t-\t-\terror:sample:read stat of pid 4242\n"
            ),
        )
        .unwrap();
        let samples = read_process_samples(&file).unwrap();
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].ppid, None);
        assert_eq!(samples[0].rss_kb, None, "a dead sample is absent, not zero");
        assert!(
            samples[0].note.contains("error:"),
            "the dead-collector note survives the reader so the row can fail on it"
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
}
