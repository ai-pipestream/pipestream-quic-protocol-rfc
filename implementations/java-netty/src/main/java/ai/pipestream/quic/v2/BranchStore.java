package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Direct-child dependency observations under a checked parent worker. These are internal execution
 * reads, not external result leases, arbitrary scope traversal or new retention promises.
 */
final class BranchStore {
  private BranchStore() {}

  /**
   * A bounded page from the parent's exact closed child scope.
   *
   * @param members ordered direct child identities, never payloads
   * @param more more identities exist beyond this page
   */
  record Page(List<WorkKey> members, boolean more) {
    /** Copy bounded identities without retaining a caller's mutable collection. */
    Page {
      if (members.size() > 256) throw ProtocolError.limit("child page capacity exceeded");
      members = List.copyOf(members);
    }
  }

  /**
   * Exact committed descriptor and local file identity, not a transferable read capability.
   *
   * @param context retained child session identity
   * @param input immutable child admission
   * @param producer historical file-producing identity, not current execution ownership
   * @param output immutable published descriptor
   */
  record Source(
      Commitments.Context context,
      InputHeader input,
      ExecutionStore.Lease producer,
      Output output) {}

  /**
   * Observe child-closure availability without occupying a worker or auditing all descendants. A
   * true result is only a scheduling hint; the committing claim verifies the complete evidence.
   *
   * @param connection stable discovery snapshot
   * @param binding retained parent session
   * @param parent checked parent view
   * @return a successful child summary is present, or this work has no children
   * @throws SQLException contradictory child allocation or unavailable storage
   */
  static boolean ready(Connection connection, Messages.Binding binding, WorkView parent)
      throws SQLException {
    if (parent.child() == null) return true;
    DeclarationStore.Scope child =
        DeclarationStore.scope(connection, binding, parent.child().scope());
    if (!parent.work().equals(child.parent()) || child.producer() != parent.child().producer())
      throw new SQLException("V2 dependency: child scope differs from parent allocation");
    ScopeSummary summary = child.state().summary();
    return summary != null && summary.counts().success() == summary.declared();
  }

  /**
   * Inspect one bounded page after checking complete STRICT child closure.
   *
   * @param connection stable metadata snapshot
   * @param binding retained parent session
   * @param parent checked current parent job and view
   * @param after exclusive lower entity bound
   * @param limit maximum returned children
   * @return exact direct child page
   * @throws SQLException contradictory retained evidence or database failure
   */
  static Page page(
      Connection connection,
      Messages.Binding binding,
      ExecutionStore.Loaded parent,
      long after,
      int limit)
      throws SQLException {
    Checks.number(after);
    if (limit < 1 || limit > 256) throw ProtocolError.limit("child page capacity exceeded");
    DeclarationStore.Scope child = scope(connection, binding, parent);
    List<WorkKey> members = new ArrayList<>(limit);
    boolean more = false;
    try (var query =
        connection.prepareStatement(
            "SELECT id FROM ps_v2_entities WHERE generation=? AND scope=? AND id>? ORDER BY id"
                + " LIMIT ?")) {
      query.setLong(1, binding.generation());
      query.setLong(2, child.id());
      query.setLong(3, after);
      query.setInt(4, limit + 1);
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          if (members.size() == limit) {
            more = true;
            break;
          }
          members.add(new WorkKey(child.id(), child.producer(), rows.getLong(1)));
        }
      }
    }
    return new Page(members, more);
  }

  /**
   * Resolve an exact committed output from the parent's own child scope. External output expiry
   * does not end this dependency; its bytes remain charged through parent settlement.
   *
   * @param connection stable metadata snapshot
   * @param config immutable application registry
   * @param installation exact authority installation
   * @param binding retained session
   * @param parent current checked parent
   * @param entity direct child identity
   * @param index published output index
   * @return descriptor and producing file identity
   * @throws SQLException contradictory retained evidence or database failure
   */
  static Source output(
      Connection connection,
      SessionStore.Configuration config,
      UUID installation,
      Messages.Binding binding,
      ExecutionStore.Loaded parent,
      long entity,
      int index)
      throws SQLException {
    Checks.id(entity);
    Checks.range(index, 0, 255);
    DeclarationStore.Scope child = scope(connection, binding, parent);
    WorkKey work = new WorkKey(child.id(), child.producer(), entity);
    ExecutionStore.Loaded retained;
    try {
      retained = ExecutionStore.load(connection, config, binding, work, (owner, parameters) -> {});
    } catch (ProtocolError refusal) {
      if (refusal.code() != ProtocolError.Code.NOT_READY) throw refusal;
      throw new SQLException("V2 dependency: closed child lost its admitted job", refusal);
    }
    WorkView view = retained.entity().view();
    JobRecord job = retained.stored().record();
    if (view.state() != State.SUCCEEDED || job.stage() != JobRecord.Stage.SETTLED)
      throw new SQLException("V2 dependency: closed successful child has no settled result");
    PublicationStore.audit(connection, binding, view, job);
    if (view.manifest() == null || index >= view.manifest().outputs().size())
      throw new ProtocolError(ProtocolError.Code.NOT_FOUND, "child output index is absent");
    if (!job.outputsLive())
      throw new SQLException("V2 dependency: live parent lost its retained child output");
    // Only immutable acquisition identity locates the file. This synthetic expiry cannot grant
    // execution or an external read lease; the enclosing transaction checks the live parent.
    ExecutionStore.Lease producer =
        new ExecutionStore.Lease(
            installation,
            binding.owner(),
            binding.generation(),
            work,
            job.attempt(),
            job.lease(),
            1);
    return new Source(
        new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
        job.input(),
        producer,
        view.manifest().outputs().get(index));
  }

  /**
   * Verify and pin the selected immutable file under already reserved reader capacity. The caller
   * holds the paired input monitor from current parent authorization through this operation.
   *
   * @param inputs exact paired file store
   * @param source current authorized dependency observation
   * @param credit this store's unborrowed reader capacity
   * @return private payload-only reader
   * @throws IOException missing or contradictory retained bytes or failed physical open
   */
  static InputStream open(InputStore inputs, Source source, OutputStore.ReaderCredit credit)
      throws IOException {
    Objects.requireNonNull(credit);
    Output expected = source.output();
    OutputStore.Stored stored =
        inputs
            .findOutput(source.context(), source.input(), source.producer(), expected.index())
            .orElseThrow(() -> new IOException("retained child output is missing"));
    if (stored.length() != expected.length()
        || !stored.sha256().equals(expected.sha256())
        || !stored.contentType().equals(expected.contentType()))
      throw new IOException("retained child output contradicts its committed descriptor");
    return stored.openStream(credit);
  }

  private static DeclarationStore.Scope scope(
      Connection connection, Messages.Binding binding, ExecutionStore.Loaded parent)
      throws SQLException {
    WorkView view = parent.entity().view();
    if (view.child() == null)
      throw new ProtocolError(ProtocolError.Code.CONFLICT, "leaf has no child scope");
    if (!parent.stored().record().expansionComplete())
      throw new ProtocolError(ProtocolError.Code.NOT_READY, "authority expansion is not complete");
    DeclarationStore.Scope child =
        DeclarationStore.scope(connection, binding, view.child().scope());
    if (!view.work().equals(child.parent()) || child.producer() != view.child().producer())
      throw new SQLException("V2 dependency: child scope differs from parent allocation");
    ScopeSummary summary = child.state().summary();
    if (summary == null)
      throw new ProtocolError(ProtocolError.Code.NOT_READY, "child scope is not closed");
    ClosureStore.verify(connection, binding, child.id());
    if (summary.counts().success() != summary.declared())
      throw new ProtocolError(ProtocolError.Code.NOT_READY, "child scope did not succeed");
    return child;
  }
}
