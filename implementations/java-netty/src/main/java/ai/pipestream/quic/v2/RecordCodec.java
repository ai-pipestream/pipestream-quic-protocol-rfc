package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.ProtocolError.*;
import static ai.pipestream.quic.v2.Records.*;

import java.util.ArrayList;
import java.util.List;

/** Exact Appendix F record positions. Parsing is schema-directed and nonrecursive. */
final class RecordCodec {
  private RecordCodec() {}

  static int small(Cbor.Reader r, int maximum) {
    return (int) Checks.range(r.number(), 0, maximum);
  }

  static Digest digest(Cbor.Reader r) {
    return new Digest(r.bytes(32));
  }

  static OperationId operation(Cbor.Reader r) {
    return new OperationId(r.bytes(16));
  }

  static WorkKey work(Cbor.Reader r) {
    r.exact(3);
    return new WorkKey(r.number(), small(r, 1), r.number());
  }

  static RequestTag tag(Cbor.Reader r) {
    r.exact(2);
    return new RequestTag(small(r, 1) == 1, r.number());
  }

  static Policy policy(Cbor.Reader r) {
    r.exact(3);
    return new Policy(r.number(), r.number(), r.number());
  }

  static Limits limits(Cbor.Reader r) {
    r.exact(6);
    return new Limits(r.number(), r.number(), r.number(), r.number(), r.number(), r.number());
  }

  static Input input(Cbor.Reader r) {
    r.exact(3);
    return new Input(r.number(), digest(r), r.text(128));
  }

  static OutputBudget budget(Cbor.Reader r) {
    r.exact(2);
    return new OutputBudget(small(r, 256), r.number());
  }

  static Diagnostic diagnostic(Cbor.Reader r) {
    r.exact(2);
    return new Diagnostic(r.number(), r.text(512));
  }

  static ChildScope child(Cbor.Reader r) {
    r.exact(2);
    return new ChildScope(r.number(), small(r, 1));
  }

  static Counts counts(Cbor.Reader r) {
    r.exact(4);
    return new Counts(r.number(), r.number(), r.number(), r.number());
  }

  static ScopeSummary summary(Cbor.Reader r) {
    r.exact(8);
    return new ScopeSummary(
        r.number(),
        small(r, 1),
        parent(r),
        digest(r),
        r.number(),
        counts(r),
        digest(r),
        r.number());
  }

  static WorkKey parent(Cbor.Reader r) {
    return r.nullable() ? null : work(r);
  }

  static Digest nullableDigest(Cbor.Reader r) {
    return r.nullable() ? null : digest(r);
  }

  static Long time(Cbor.Reader r) {
    return r.nullable() ? null : r.number();
  }

  static AdmitParameters admit(Cbor.Reader r) {
    r.exact(6);
    return new AdmitParameters(work(r), input(r), r.text(128), small(r, 2), r.number(), budget(r));
  }

  static InputHeader inputHeader(Cbor.Reader r) {
    r.exact(4);
    require(r.number() == 0, "wrong input header kind");
    return new InputHeader(r.number(), operation(r), admit(r));
  }

  static ResultHeader resultHeader(Cbor.Reader r) {
    r.exact(8);
    require(r.number() == 1, "wrong result header kind");
    return new ResultHeader(
        r.number(), r.number(), work(r), r.number(), small(r, 255), r.number(), digest(r));
  }

  static Output output(Cbor.Reader r) {
    r.exact(5);
    return new Output(small(r, 255), r.number(), digest(r), r.text(128), new Locator(r.text(1024)));
  }

  static Manifest manifest(Cbor.Reader r) {
    r.exact(10);
    require(r.number() == 2, "wrong manifest version");
    String authority = r.text(128), owner = r.text(128);
    long generation = r.number();
    WorkKey work = work(r);
    long attempt = r.number();
    Digest input = digest(r);
    long committed = r.number(), available = r.number();
    int count = r.array(256);
    List<Output> outputs = new ArrayList<>(count);
    for (int n = 0; n < count; n++) outputs.add(output(r));
    return new Manifest(
        authority, owner, generation, work, attempt, input, committed, available, outputs);
  }

  static WorkView view(Cbor.Reader r) {
    r.exact(12);
    return new WorkView(
        work(r),
        State.from(r.number()),
        r.number(),
        r.nullable() ? null : input(r),
        time(r),
        time(r),
        time(r),
        time(r),
        time(r),
        r.nullable() ? null : child(r),
        r.nullable() ? null : manifest(r),
        r.nullable() ? null : diagnostic(r));
  }

  static OperationReceipt receipt(Cbor.Reader r) {
    r.exact(3);
    return new OperationReceipt(operation(r), digest(r), outcome(r));
  }

  static Outcome outcome(Cbor.Reader r) {
    int fields = r.array(6), op = small(r, 5);
    switch (op) {
      case 0:
        require(fields == 6, "wrong admission outcome length");
        return new Admitted(
            work(r), r.number(), r.number(), r.number(), r.nullable() ? null : child(r));
      case 1:
        require(fields == 6, "wrong declaration outcome length");
        return new Declared(r.number(), small(r, 1), small(r, 256), r.number(), nullableDigest(r));
      case 2:
        require(fields == 5, "wrong retry outcome length");
        return new Retried(work(r), r.number(), r.number(), r.number());
      case 3:
        require(fields == 5, "wrong cancel outcome length");
        return new Cancelled(work(r), r.number(), small(r, 1), State.from(r.number()));
      case 4:
        require(fields == 3, "wrong scope cancellation outcome length");
        return new ScopeCancelled(r.number(), r.number());
      case 5:
        require(fields == 5, "wrong skip outcome length");
        return new Skipped(work(r), r.number(), small(r, 1), State.from(r.number()));
      default:
        throw frame("unknown operation outcome");
    }
  }

  static void nullable(Cbor.Writer w, Value value) {
    if (value == null) w.nil();
    else write(w, value);
  }

  static void nullable(Cbor.Writer w, Digest value) {
    if (value == null) w.nil();
    else w.bytes(value.bytes());
  }

  static void time(Cbor.Writer w, Long value) {
    if (value == null) w.nil();
    else w.number(value);
  }

  static void start(Cbor.Writer w, int fields, int op) {
    w.array(fields);
    w.number(op);
  }

  static void write(Cbor.Writer w, Value value) {
    switch (value) {
      case WorkKey v -> {
        w.array(3);
        w.number(v.scope());
        w.number(v.producer());
        w.number(v.entity());
      }
      case RequestTag v -> {
        w.array(2);
        w.number(v.input() ? 1 : 0);
        w.number(v.id());
      }
      case Policy v -> {
        w.array(3);
        w.number(v.executionLimit());
        w.number(v.outputRetention());
        w.number(v.receiptRetention());
      }
      case Limits v -> {
        w.array(6);
        w.number(v.scopes());
        w.number(v.entities());
        w.number(v.operations());
        w.number(v.inputBytes());
        w.number(v.outputBytes());
        w.number(v.activeJobs());
      }
      case Input v -> {
        w.array(3);
        w.number(v.length());
        w.bytes(v.sha256().bytes());
        w.text(v.contentType(), 128);
      }
      case OutputBudget v -> {
        w.array(2);
        w.number(v.count());
        w.number(v.totalBytes());
      }
      case Diagnostic v -> {
        w.array(2);
        w.number(v.code());
        w.text(v.detail(), 512);
      }
      case ChildScope v -> {
        w.array(2);
        w.number(v.scope());
        w.number(v.producer());
      }
      case Counts v -> {
        w.array(4);
        w.number(v.success());
        w.number(v.failure());
        w.number(v.cancelled());
        w.number(v.skipped());
      }
      case ScopeSummary v -> {
        w.array(8);
        w.number(v.scope());
        w.number(v.producer());
        nullable(w, v.parent());
        w.bytes(v.seal().bytes());
        w.number(v.declared());
        write(w, v.counts());
        w.bytes(v.statusRoot().bytes());
        w.number(v.closedAt());
      }
      case AdmitParameters v -> {
        w.array(6);
        write(w, v.work());
        write(w, v.input());
        w.text(v.application(), 128);
        w.number(v.mode());
        w.number(v.executionMs());
        write(w, v.outputs());
      }
      case InputHeader v -> {
        start(w, 4, 0);
        w.number(v.generation());
        w.bytes(v.operation().bytes());
        write(w, v.parameters());
      }
      case ResultHeader v -> {
        start(w, 8, 1);
        w.number(v.request());
        w.number(v.generation());
        write(w, v.work());
        w.number(v.attempt());
        w.number(v.index());
        w.number(v.length());
        w.bytes(v.sha256().bytes());
      }
      case Output v -> {
        w.array(5);
        w.number(v.index());
        w.number(v.length());
        w.bytes(v.sha256().bytes());
        w.text(v.contentType(), 128);
        w.text(v.locator().value(), 1024);
      }
      case Manifest v -> {
        start(w, 10, 2);
        w.text(v.authority(), 128);
        w.text(v.owner(), 128);
        w.number(v.generation());
        write(w, v.work());
        w.number(v.attempt());
        w.bytes(v.inputSha256().bytes());
        w.number(v.committedAt());
        w.number(v.availableUntil());
        w.array(v.outputs().size());
        for (Output output : v.outputs()) write(w, output);
      }
      case WorkView v -> {
        w.array(12);
        write(w, v.work());
        w.number(v.state().value());
        w.number(v.attempt());
        nullable(w, v.input());
        time(w, v.admittedAt());
        time(w, v.deadline());
        time(w, v.terminalAt());
        time(w, v.receiptUntil());
        time(w, v.outputUntil());
        nullable(w, v.child());
        nullable(w, v.manifest());
        nullable(w, v.diagnostic());
      }
      case OperationReceipt v -> {
        w.array(3);
        w.bytes(v.operation().bytes());
        w.bytes(v.requestDigest().bytes());
        write(w, v.outcome());
      }
      case Admitted v -> {
        start(w, 6, 0);
        write(w, v.work());
        w.number(v.attempt());
        w.number(v.admittedAt());
        w.number(v.deadline());
        nullable(w, v.child());
      }
      case Declared v -> {
        start(w, 6, 1);
        w.number(v.scope());
        w.number(v.producer());
        w.number(v.acceptedCount());
        w.number(v.declared());
        nullable(w, v.seal());
      }
      case Retried v -> {
        start(w, 5, 2);
        write(w, v.work());
        w.number(v.expectedAttempt());
        w.number(v.replacementAttempt());
        w.number(v.acceptedAt());
      }
      case Cancelled v -> {
        start(w, 5, 3);
        write(w, v.work());
        w.number(v.acceptedAt());
        w.number(v.disposition());
        w.number(v.state().value());
      }
      case ScopeCancelled v -> {
        start(w, 3, 4);
        w.number(v.scope());
        w.number(v.acceptedAt());
      }
      case Skipped v -> {
        start(w, 5, 5);
        write(w, v.work());
        w.number(v.acceptedAt());
        w.number(v.disposition());
        w.number(v.state().value());
      }
    }
  }
}
