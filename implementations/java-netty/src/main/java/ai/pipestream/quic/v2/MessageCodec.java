package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.ProtocolError.*;
import static ai.pipestream.quic.v2.RecordCodec.*;
import static ai.pipestream.quic.v2.Records.*;

import java.util.ArrayList;
import java.util.List;

final class MessageCodec {
  private MessageCodec() {}

  static List<Integer> extensions(Cbor.Reader r) {
    int count = r.array(32);
    List<Integer> ids = new ArrayList<>(count);
    for (int n = 0; n < count; n++) ids.add(small(r, 65534));
    return ids;
  }

  static Message read(int type, Cbor.Reader r) {
    if (type == 1) {
      r.exact(9);
      return new Capabilities(
          small(r, 1) == 1,
          extensions(r),
          extensions(r),
          small(r, 1048576),
          small(r, 1024),
          small(r, 1024),
          r.number(),
          r.number(),
          r.number());
    }
    if (type == 7) {
      r.exact(3);
      return new Refusal(tag(r), Code.from(r.number()), r.text(512));
    }
    int fields = r.array(10), op = small(r, 11);
    return switch (type) {
      case 2 -> session(r, fields, op);
      case 3 -> scope(r, fields, op);
      case 4 -> workMessage(r, fields, op);
      case 5 -> result(r, fields, op);
      case 6 -> drain(r, fields, op);
      default -> throw frame("unknown control type");
    };
  }

  private static void fields(int actual, int expected) {
    require(actual == expected, "wrong message array cardinality");
  }

  private static Message session(Cbor.Reader r, int n, int op) {
    switch (op) {
      case 0:
        fields(n, 4);
        return new Create(r.number(), r.number(), policy(r));
      case 1:
        fields(n, 8);
        return new Binding(
            r.number(), r.text(128), r.text(128), r.number(), r.number(), policy(r), limits(r));
      case 2:
        fields(n, 5);
        return new Attach(r.number(), r.text(128), r.text(128), r.number());
      case 3:
        fields(n, 2);
        return new NextSequence(r.number());
      case 4:
        fields(n, 3);
        return new Sequence(r.number(), r.number());
      default:
        throw frame("unknown SESSION operation");
    }
  }

  private static Message scope(Cbor.Reader r, int n, int op) {
    switch (op) {
      case 0:
        {
          fields(n, 6);
          long request = r.number();
          OperationId operation = operation(r);
          long scope = r.number();
          int count = r.array(256);
          List<Long> ids = new ArrayList<>(count);
          for (int i = 0; i < count; i++) ids.add(r.number());
          return new Declare(request, operation, scope, ids, r.bool());
        }
      case 1:
        fields(n, 3);
        return new DeclarationResponse(r.number(), receipt(r));
      case 2:
        fields(n, 5);
        return new Page(r.number(), r.number(), r.number(), small(r, 256));
      case 3:
        {
          fields(n, 10);
          long request = r.number(), scope = r.number();
          int producer = small(r, 1);
          WorkKey parent = parent(r);
          boolean sealed = r.bool();
          Digest seal = nullableDigest(r);
          long declared = r.number();
          int count = r.array(256);
          List<Entry> entries = new ArrayList<>(count);
          for (int i = 0; i < count; i++) {
            r.exact(2);
            entries.add(new Entry(r.number(), State.from(r.number())));
          }
          return new PageResponse(
              request, scope, producer, parent, sealed, seal, declared, entries, r.bool());
        }
      case 4:
        fields(n, 5);
        return new Checkpoint(r.number(), r.number(), digest(r), r.number());
      case 5:
        fields(n, 3);
        return new CheckpointResponse(r.number(), summary(r));
      case 6:
        fields(n, 4);
        return new CancelScope(r.number(), operation(r), r.number());
      case 7:
        fields(n, 3);
        return new CancelScopeResponse(r.number(), receipt(r));
      default:
        throw frame("unknown SCOPE operation");
    }
  }

  private static Message workMessage(Cbor.Reader r, int n, int op) {
    switch (op) {
      case 1:
        fields(n, 3);
        return new AdmissionResponse(tag(r), receipt(r));
      case 2:
        fields(n, 3);
        return new LookupOperation(r.number(), operation(r));
      case 3:
        fields(n, 3);
        return new OperationResponse(r.number(), receipt(r));
      case 4:
        fields(n, 5);
        return new Watch(r.number(), work(r), r.number(), r.number());
      case 5:
        fields(n, 4);
        return new WatchResponse(r.number(), r.number(), view(r));
      case 6:
        fields(n, 5);
        return new Retry(r.number(), operation(r), work(r), r.number());
      case 7:
        fields(n, 3);
        return new RetryResponse(r.number(), receipt(r));
      case 8:
        fields(n, 4);
        return new Cancel(r.number(), operation(r), work(r));
      case 9:
        fields(n, 3);
        return new CancelResponse(r.number(), receipt(r));
      case 10:
        fields(n, 4);
        return new Skip(r.number(), operation(r), work(r));
      case 11:
        fields(n, 3);
        return new SkipResponse(r.number(), receipt(r));
      default:
        throw frame("unknown WORK operation");
    }
  }

  private static Message result(Cbor.Reader r, int n, int op) {
    switch (op) {
      case 0:
        fields(n, 6);
        return new Read(r.number(), work(r), r.number(), small(r, 255), digest(r));
      case 1:
        fields(n, 4);
        return new GetManifest(r.number(), work(r), r.number());
      case 2:
        fields(n, 3);
        return new ManifestResponse(r.number(), manifest(r));
      default:
        throw frame("unknown RESULT operation");
    }
  }

  private static Message drain(Cbor.Reader r, int n, int op) {
    switch (op) {
      case 0:
        fields(n, 4);
        return new Complete(r.number(), r.number(), summary(r));
      case 1:
        fields(n, 4);
        return new Completed(r.number(), r.number(), summary(r));
      case 2:
        fields(n, 2);
        return new Detach(r.number());
      case 3:
        fields(n, 2);
        return new Detached(r.number());
      default:
        throw frame("unknown DRAIN operation");
    }
  }

  private static void prefix(Cbor.Writer w, int fields, int operation, long request) {
    start(w, fields, operation);
    w.number(request);
  }

  private static void extensions(Cbor.Writer w, List<Integer> values) {
    w.array(values.size());
    for (int value : values) w.number(value);
  }

  static void write(Cbor.Writer w, Message message) {
    switch (message) {
      case Capabilities v -> {
        w.array(9);
        w.number(v.response() ? 1 : 0);
        extensions(w, v.supported());
        extensions(w, v.required());
        w.number(v.controlLimit());
        w.number(v.streamLimit());
        w.number(v.pendingLimit());
        w.number(v.objectLimit());
        w.number(v.streamIdleMs());
        w.number(v.streamLifetimeMs());
      }
      case Create v -> {
        prefix(w, 4, 0, v.request());
        w.number(v.creationSequence());
        RecordCodec.write(w, v.policy());
      }
      case Binding v -> {
        prefix(w, 8, 1, v.request());
        w.text(v.authority(), 128);
        w.text(v.owner(), 128);
        w.number(v.generation());
        w.number(v.creationSequence());
        RecordCodec.write(w, v.policy());
        RecordCodec.write(w, v.limits());
      }
      case Attach v -> {
        prefix(w, 5, 2, v.request());
        w.text(v.authority(), 128);
        w.text(v.owner(), 128);
        w.number(v.generation());
      }
      case NextSequence v -> prefix(w, 2, 3, v.request());
      case Sequence v -> {
        prefix(w, 3, 4, v.request());
        w.number(v.nextCreationSequence());
      }
      case Declare v -> {
        prefix(w, 6, 0, v.request());
        w.bytes(v.operation().bytes());
        w.number(v.scope());
        w.array(v.entityIds().size());
        for (long id : v.entityIds()) w.number(id);
        w.bool(v.seal());
      }
      case DeclarationResponse v -> {
        prefix(w, 3, 1, v.request());
        RecordCodec.write(w, v.receipt());
      }
      case Page v -> {
        prefix(w, 5, 2, v.request());
        w.number(v.scope());
        w.number(v.afterEntity());
        w.number(v.limit());
      }
      case PageResponse v -> {
        prefix(w, 10, 3, v.request());
        w.number(v.scope());
        w.number(v.producer());
        nullable(w, v.parent());
        w.bool(v.sealed());
        nullable(w, v.seal());
        w.number(v.declared());
        w.array(v.entries().size());
        for (Entry e : v.entries()) {
          w.array(2);
          w.number(e.entity());
          w.number(e.state().value());
        }
        w.bool(v.more());
      }
      case Checkpoint v -> {
        prefix(w, 5, 4, v.request());
        w.number(v.scope());
        w.bytes(v.seal().bytes());
        w.number(v.waitMs());
      }
      case CheckpointResponse v -> {
        prefix(w, 3, 5, v.request());
        RecordCodec.write(w, v.summary());
      }
      case CancelScope v -> {
        prefix(w, 4, 6, v.request());
        w.bytes(v.operation().bytes());
        w.number(v.scope());
      }
      case CancelScopeResponse v -> {
        prefix(w, 3, 7, v.request());
        RecordCodec.write(w, v.receipt());
      }
      case AdmissionResponse v -> {
        start(w, 3, 1);
        RecordCodec.write(w, v.request());
        RecordCodec.write(w, v.receipt());
      }
      case LookupOperation v -> {
        prefix(w, 3, 2, v.request());
        w.bytes(v.operation().bytes());
      }
      case OperationResponse v -> {
        prefix(w, 3, 3, v.request());
        RecordCodec.write(w, v.receipt());
      }
      case Watch v -> {
        prefix(w, 5, 4, v.request());
        RecordCodec.write(w, v.work());
        w.number(v.afterRevision());
        w.number(v.waitMs());
      }
      case WatchResponse v -> {
        prefix(w, 4, 5, v.request());
        w.number(v.revision());
        RecordCodec.write(w, v.work());
      }
      case Retry v -> {
        prefix(w, 5, 6, v.request());
        w.bytes(v.operation().bytes());
        RecordCodec.write(w, v.work());
        w.number(v.expectedAttempt());
      }
      case RetryResponse v -> {
        prefix(w, 3, 7, v.request());
        RecordCodec.write(w, v.receipt());
      }
      case Cancel v -> {
        prefix(w, 4, 8, v.request());
        w.bytes(v.operation().bytes());
        RecordCodec.write(w, v.work());
      }
      case CancelResponse v -> {
        prefix(w, 3, 9, v.request());
        RecordCodec.write(w, v.receipt());
      }
      case Skip v -> {
        prefix(w, 4, 10, v.request());
        w.bytes(v.operation().bytes());
        RecordCodec.write(w, v.work());
      }
      case SkipResponse v -> {
        prefix(w, 3, 11, v.request());
        RecordCodec.write(w, v.receipt());
      }
      case Read v -> {
        prefix(w, 6, 0, v.request());
        RecordCodec.write(w, v.work());
        w.number(v.attempt());
        w.number(v.index());
        w.bytes(v.expectedSha256().bytes());
      }
      case GetManifest v -> {
        prefix(w, 4, 1, v.request());
        RecordCodec.write(w, v.work());
        w.number(v.attempt());
      }
      case ManifestResponse v -> {
        prefix(w, 3, 2, v.request());
        RecordCodec.write(w, v.manifest());
      }
      case Complete v -> {
        prefix(w, 4, 0, v.request());
        w.number(v.generation());
        RecordCodec.write(w, v.root());
      }
      case Completed v -> {
        prefix(w, 4, 1, v.request());
        w.number(v.generation());
        RecordCodec.write(w, v.root());
      }
      case Detach v -> prefix(w, 2, 2, v.request());
      case Detached v -> prefix(w, 2, 3, v.request());
      case Refusal v -> {
        w.array(3);
        RecordCodec.write(w, v.request());
        w.number(v.code().value());
        w.text(v.detail(), 512);
      }
    }
  }
}
