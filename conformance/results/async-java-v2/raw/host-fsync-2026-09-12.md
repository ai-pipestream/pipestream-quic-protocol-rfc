# Host storage note, 2026-09-12

The full 744-test run at `ce1bfd77` could not be made clean on this host. This
note records why, so the failures in `full-offline-2026-09-11f.summary.log`
are read as host-attributed and not re-investigated as code.

## Symptom

`fsync` after a small write on `/work` (XFS on `md0`, RAID-0 of three Crucial
CT2000T710SSD8, firmware PBCR5103) costs 31 ms, constant to 0.1 ms, in runs;
occasionally runs of 2.5 ms. Before 2026-09-11 the same call cost about 1 ms.
The failing tests are the lease-interval and bounded-deadline classes
(`AuthorityExpansion*`, `BranchExecution*`, `BranchScheduler*`,
`SealedServerTest` checkpoints): "local monotonic lease interval ended" and
"condition not reached before bounded deadline". They fail identically on
`4fe9fff2` under the same conditions (A/B in a scratch worktree).

## Measurements (in-process, `IO::Handle->sync`, 2026-09-12 02:00-02:40Z)

| operation on `/work` | latency |
|---|---|
| write 4 KiB or 64 KiB, then fsync | 31.3 ms |
| write 2 MiB or more, then fsync | 3 to 4 ms |
| 4 KiB write with O_DSYNC (FUA) | 2.5 ms |
| lone `nvme flush` | about 0.5 ms |
| 4 KiB direct read | about 50 us |
| 1 MiB direct sequential writes | 4.2 GB/s |
| two processes flushing concurrently | 62 ms each |

The Samsung 990 PRO root drive (ext4) shows 4.5 to 5.5 ms per fsync regardless
of size.

## Ruled out, each by test

Reboot; NVMe autonomous power states (drives sat in PS4, forcing PS0 and
disabling APST changed nothing); PCIe ASPM L1 (disabled, no change); interrupt
coalescing (off); interrupt delivery per CPU chiplet (reads fast from both);
cgroup I/O limits (none); PCIe AER counters (zero); temperature and SMART
(clean, wear 1 percent); kernel change (same kernel was fast on 2026-09-10 while
other jobs kept the drives streaming writes); `fstrim` (run, no change); small
direct-write trickles (no change); disabling the drives' volatile write cache
(worse: 34 to 94 ms fsync, 224 MB/s direct writes; reverted). No firmware
update for the T710 on the vendor feed.

## Conclusion

The drive firmware commits a partially filled flash stripe on every flush that
follows a small write. Flush-only workloads (SQLite journals, this suite's
durable stores, the conformance driver's authorities) take the slow path; a
box that is writing continuously masks it. Timing gates on `/work` stay noisy
until `/work` moves to a drive without this behaviour.

## Mitigation that works (2026-09-12 02:26Z)

Every test store lives under JUnit's `@TempDir`, which follows
`java.io.tmpdir`. Pointing it at the Samsung root drive
(`JAVA_TOOL_OPTIONS=-Djava.io.tmpdir=/home/krickert/.rfc-tmp`, no pom change)
gives 5 ms per fsync instead of 31 ms, and the full suite at `2ce4d318`
passed 744/744 in ten minutes (`full-offline-2026-09-12.summary.log`) where
the same tree on `/work` took forty minutes and failed 21 timing tests. The
same move applies to the conformance driver's and the workload's authority
directories: keep them off `/work` until it is on a drive that flushes
properly.
