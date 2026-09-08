# Java V2 transport credit review

Status: 2026-09-07. The Java reference and external Java example now use the
source-pinned QUIC extension with the maintained Netty 4.2.17.Final BOM. The
official-artifact migration below precedes this extension. Dependency integration
does not prove durable object transport or its complete control reservation.
The connection-confined Java stream owner now installs native limits in both
Core endpoints and separately implements bounded object-stream admission.
Java V2 still advertises Core only. The full durable-work/results goal and its
independent failure driver and equivalent gRPC workload remain open.

## Exact dependency boundary

The [incubator repository](https://github.com/netty/netty-incubator-codec-quic)
was archived on 2026-05-08 and directs users to Netty 4.2. Both Maven projects
import `io.netty:netty-bom:4.2.17.Final`. At the migration checkpoint, the reference used
`io.netty:netty-codec-classes-quic` and the `linux-x86_64` runtime classifier of
`io.netty:netty-codec-native-quic`. QUIC APIs use
`io.netty.handler.codec.quic`. Each migrated NIO owner still has one event-loop
thread, created with `MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())`.
No protocol messages, persistent formats or test assertions changed in that
mechanical migration.

The full regression run then found three obsolete exception assertions in the
V1 certificate-name tests. Netty 4.2
[enables HTTPS endpoint identification by default](https://github.com/netty/netty/blob/netty-4.2.17.Final/handler/src/main/java/io/netty/handler/ssl/SslContext.java),
so a wrong name can fail the handshake before the existing explicit SAN check.
Both V1 clients now select that algorithm explicitly, independent of Netty's
global default override. The stricter PipeStream SAN check remains mandatory
before application frames, including its prohibition on Common Name fallback.
The regression oracle must recognize a certificate-verification handshake failure
or the precise SAN refusal, not any exception or timeout, and verify that invalid
identity sends no protocol data. This is earlier verification, not a reason to
disable native hostname checking to preserve the old exception type.

The resolved native JAR manifest identifies
`META-INF/native/libnetty_quiche42_linux_x86_64.so`, quiche revision
`4f347477006bf7f928335d28f05056013f70b87e`, and BoringSSL revision
`0226f30467f540a3f62ef48d453f93927da199b6`. These native upstream revisions are
unchanged from the previously pinned incubator build; changing the Maven version
is not evidence of a newer underlying TLS/QUIC engine. Artifact hashes and test
commands are recorded in the
[conformance evidence](../../conformance/results/durable-work-v2-java-netty42-2026-09-07.txt).

## Receive credit is a continuing invariant

The exact Netty
[builder](https://github.com/netty/netty/blob/netty-4.2.17.Final/codec-classes-quic/src/main/java/io/netty/handler/codec/quic/QuicCodecBuilder.java)
exposes initial connection and stream credit. Its
[JNI bridge](https://github.com/netty/netty/blob/netty-4.2.17.Final/codec-native-quic/src/main/c/netty_quic_quiche.c)
does not expose quiche's maximum connection/stream receive-window setters.
Large initial MAX_DATA and small initial stream credit therefore do not establish
a fixed data budget for the connection's lifetime.

In the bundled quiche
[connection implementation](https://github.com/cloudflare/quiche/blob/4f347477006bf7f928335d28f05056013f70b87e/quiche/src/lib.rs),
the initial replenishment window is
`min(initial_max_data / 2 * 3, 48 KiB)`, distinct from the initial advertised
connection limit. The default maximum connection window is 24 MiB and default
maximum stream window is 16 MiB. A stream-window update can raise the connection
window's lower bound to 1.5 times that stream's receive window.

The bundled
[flow controller](https://github.com/cloudflare/quiche/blob/4f347477006bf7f928335d28f05056013f70b87e/quiche/src/flowcontrol.rs)
requests a credit update when remaining credit falls below half its current
window. The next limit is consumed bytes plus the current window. Closely spaced
updates can double the window, up to its configured maximum. These source facts
invalidate an argument based solely on configured initial values; they are not a
measured deadlock or memory result for a Java durable endpoint that does not yet
exist.

The pinned packet builder also marks connection credit for update whenever it
successfully emits a MAX_STREAM_DATA frame. It tries MAX_DATA after the stream
updates, and a frame that does not fit remains pending for a later packet. That
coupling helps progress but is not atomic delivery of the two limits. Tests of
replacement streams must therefore exercise the packet-boundary ordering as well
as the half-window threshold, rather than assuming that observing fresh stream
credit implies fresh connection credit was received in the same packet.

For N concurrently unread data streams, a bound must cover their effective
windows, independent control credit, and consumed bytes awaiting a connection
credit update. It must hold after consumption, autotuning, reset, retirement and
replacement. Merely exposing the two maximum-window setters would still require
checking initial/replenishment geometry and update batching. Section 12.1 now
makes the autotuning requirement explicit without prescribing a particular QUIC
implementation or numerical window.

## Local send completion is not buffer release

The same quiche connection implementation tracks buffered and unacknowledged
stream bytes separately from its send-capacity calculation. Send capacity depends
on congestion-window availability and the peer's remaining connection credit; it
is not a configurable application data-only send-buffer reservation.

Netty's
[stream channel](https://github.com/netty/netty/blob/netty-4.2.17.Final/codec-classes-quic/src/main/java/io/netty/handler/codec/quic/QuicheQuicStreamChannel.java)
completes a write when native stream-send accepts the buffer. That is not peer
acknowledgment. `bytesBeforeUnwritable()` returns cached stream capacity, not an
atomic reservation of the connection's shared data budget. Application queue
counters, stream priority and packet statistics cannot independently prove that
transport-owned data leaves protected capacity for control.

The Java object owner needs a supported transport admission mechanism with
documented accounting/release semantics, bounded application queues, and an
independent control wakeup/deadline path. It must not free charged transport data
merely because Netty accepted an application write. Section 12.1 now states this
accounting rule. A dependency extension must be reproducibly built and pinned;
reflection into native connection pointers or an unpublished local dependency
override is not an implementation plan.

## Source-pinned transport extension

The [transport patch bundle](../../implementations/java-netty/transport/README.md)
now implements the missing APIs against the exact source revisions above. It is
a separately identified dependency build, not an upstream release. The Java
reference POM now selects the extension's exact coordinates and rejects
official/incubator QUIC duplicates. Its source bootstrap verifies native tests,
installs into an isolated Maven repository, and returns that path only after
success; the reference and example use the same repository. No V2 durable endpoint
or end-to-end resource claim follows from the native transport tests.

The receive setters expose the initial replenishment window and maximum
connection/stream windows. An explicitly configured initial window is clamped
at connection creation, independent of setter order; unset configuration keeps
the upstream default. Stream-driven lower bounds and already advertised credit
still apply. The setters do not themselves prove a receive-memory ceiling.

The send extension counts each live stream's written offset beyond its
contiguous ACK frontier. ACKed tails behind a hole remain charged. Saturation
fails closed across JNI. Ordinary streams cannot consume the reserved slice of
the connection-wide send allowance, including on automatic queued-write retries;
only locally classified control streams may use it. Application write completion
does not release native charge. Reset releases cleared buffers, not another
stream's allowance. This is a logical span, not exact allocated bytes: partial
backing-buffer prefixes, QUIC metadata, TLS and application queues need separate
limits. Scanning all live streams is acceptable only with an explicit bound on
their count.

Native loopback tests found two additional owner defects under backpressure:
graceful output shutdown could send FIN ahead of queued payload, and local reset
could leave queued write promises pending. The extension queues FIN in order and
fails locally reset writes with a named output-shutdown exception. Tests require
exact composite-buffer bytes, FIN only after all bytes, failed pending and future
reset writes, and reopened allowance for another stream. A zero-reservation
negative control distinguishes protected allowance from ordinary native progress.

These are transport admission/cleanup tests, not the complete five gates below.
The subsequent Core/stream-owner tests exposed another native defect: all payload
bytes could be acknowledged before a later empty FIN. If the application then
consumed the peer's FIN before the next send, the native stream could be collected
without transmitting its own FIN. A retained native QLOG and a deterministic
packet-level red-to-green regression identify that arrival order; extending the
PipeStream timeout would not correct it. The `.2` dependency revision tracks FIN
acknowledgment separately, including empty streams, payload ACK gaps, duplicate
FIN writes and reset abandonment. This restores the send-side distinction in
[RFC 9000 Section 3.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-3.1),
without redefining PipeStream detach or durable outcomes.

Actual PipeStream control messages, deadline scheduling under withheld peer
credit, repeated receive-window growth/replacement, durable ownership and measured
whole-process bounds require the five owner acceptance gates below. The dependency
is integrated through its reproducible pinned build with no co-loaded official
QUIC classes or system-path dependency. Full protocol regression and object-owner
resource tests remain separate from dependency identity and native transport tests.

## Java connection-owned stream admission

`StreamTransport` now binds reserved native admission to actual client-created
bidirectional Stream 0, with one owner per connection. It verifies that the
connection's native limits were installed before registration. Incoming and
outgoing objects have separate active-slot ceilings. Open operations reserve
before stream creation, and a cancelled caller view cannot cancel or free the
underlying operation. Local writes copy at most one configured chunk per outgoing
stream and remain charged until the actual Netty future settles. Snapshots report
this application queue separately from native retained spans.

FIN waits behind that stream's pending bytes. Reset/STOP uses the stream's actual
direction. Local FIN/reset does not automatically release a protocol request's
slot, complete its receipt, or assert durable work state. The next durable owner
must retain input correlation even after transport cleanup. A finite lifetime
stream ordinal/creation ceiling prevents indefinite native stream-history growth
despite early Java-channel retirement; it is not an advertised durable quota.

Receive-only streams use explicit QUIC frame reception: Netty does not supply
half-closure events on those streams, and channel inactivity is not evidence of
FIN. Reads are manual, with one fixed-size message per read cycle. The future
verified-object receiver must reserve its staging/worker capacity before requesting
the next read and hold any asynchronous consumer's buffer ownership until release.

The initial/replenishment/maximum connection window is `2*(N+1)*W`, and every
receive stream's initial and maximum window is W. The stream-driven 1.5W lower
bound fits this geometry, including N=0 Core connections. This explicit geometry
still needs adversarial credit-frame ordering, repeated replacement and measured
resource tests; it is not an end-to-end proof. `ControlWrites` has a timer-driven
native retry using the same reserved classification, independent of data reads.
The stream owner bounds each open/write/FIN wait separately; these local admission
deadlines do not replace negotiated payload or correlated-response deadlines.

## Required acceptance before durable object integration

1. Actual QUIC control requests/responses cross while every allowed data stream
   is stalled at its receive limit, in both directions. Exercise replenishment,
   window growth, reset and repeated stream replacement, not just initial credit.
2. Saturate data send admission, including transport-owned outstanding bytes.
   Control must retain its own allowance and wakeup. Removing the reservation
   must make the targeted regression fail; an observation timeout is not a named
   protocol refusal or success.
3. Objects larger than the windows finish incrementally with exact bytes and FIN
   once consumers resume. Refusal/reset releases only its owned resources and
   cannot manufacture an admission receipt, result, or completed drain.
4. A peer withholding control credit or a network that cannot deliver packets
   reaches the bounded control deadline. Other connections progress; no test
   treats successful local writes as peer delivery.
5. Measure application queues, Java heap, native/total process memory and the
   actual transport limits separately, under repeated load and cleanup. Core
   connection-count limits or a small JVM heap do not prove total-memory bounds.

The existing Rust reservation tests remain Rust evidence. Java Core/TLS and V1
interop regression tests protect the migration, but cannot satisfy these V2
durable-object gates. Next implementation work must close all five owner gates
alongside the independent Java durable store/execution/result
and recovery layers, not remove or weaken the required profiles.
