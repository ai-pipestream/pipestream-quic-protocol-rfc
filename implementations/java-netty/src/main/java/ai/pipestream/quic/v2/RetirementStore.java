package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/** Independent Java retirement eligibility and bounded metadata cleanup under paired ownership. */
final class RetirementStore {
  /** Local progress, never an authoritative wire outcome. */
  enum State {
    /** A closure, time promise or logical resource still prevents retirement. */
    NOT_READY,
    /** Physical storage or reception still belongs to the session. */
    PINNED,
    /** Immutable retirement intent committed; subsequent access expires. */
    STARTED,
    /** Some bounded metadata cleanup committed, with the root and proof retained. */
    IN_PROGRESS,
    /** Final root, proof and session deletion committed atomically. */
    COMPLETE,
    /** No session remains at this generation; allocators are unchanged. */
    ABSENT
  }

  /** Committed observation boundaries for real process-death tests. */
  enum Phase {
    /** Immutable eligibility and retirement fence committed. */
    INTENT_COMMITTED,
    /** One refunded job and its private slot were removed. */
    JOB_REMOVED,
    /** One historical operation was removed. */
    OPERATION_REMOVED,
    /** One terminal member and its work/fence slots were removed. */
    ENTITY_REMOVED,
    /** One non-root closed scope and its slot were removed. */
    SCOPE_REMOVED,
    /** Root, proof and session were removed together. */
    FINISHED
  }

  /** Trusted local instrumentation, never a peer callback. */
  @FunctionalInterface
  interface Probe {
    /**
     * Observe a committed boundary without authorizing further mutation.
     *
     * @param phase completed durable boundary
     * @throws SQLException observation failure; the preceding commit remains authoritative
     */
    void at(Phase phase) throws SQLException;
  }

  /**
   * Directly committed cleanup counts for one bounded call.
   *
   * @param state current retirement state
   * @param jobs job bundles deleted
   * @param operations operation rows deleted
   * @param entities member bundles deleted
   * @param scopes non-root scope bundles deleted
   */
  record Progress(State state, int jobs, int operations, int entities, int scopes) {}

  /**
   * Finite local discovery continuation, not authority to expire any session.
   *
   * @param after last examined generation, exclusive
   * @param through fixed inclusive generation high-water mark for this sweep
   */
  record ScanCursor(long after, long through) {
    /** Reject malformed scan bounds. */
    ScanCursor {
      Checks.number(after);
      Checks.id(through);
      ProtocolError.require(after < through, "retirement cursor outside finite sweep");
    }
  }

  /**
   * Bounded closed/retiring session hints; cleanup must revalidate each independently.
   *
   * @param generations closed or retiring generations within the examined page
   * @param examined total session rows examined, including not-yet-closed sessions
   * @param next continuation, or null when this finite sweep ended
   */
  record Page(List<Long> generations, int examined, ScanCursor next) {
    /** Keep the bounded observations immutable. */
    Page {
      generations = List.copyOf(generations);
    }
  }

  private RetirementStore() {}

  /**
   * Load immutable eligibility, checking flag/proof agreement and retained authority identity. This
   * validates the root without requiring already deleted members to reappear.
   *
   * @param connection checked metadata snapshot
   * @param binding retained immutable creation receipt
   * @param marked session retirement flag
   * @param slot nullable proof ownership link
   * @return verified proof, or null for an unmarked live session
   * @throws SQLException malformed, missing or contradictory eligibility
   */
  static RetirementRecord load(
      Connection connection, Messages.Binding binding, boolean marked, Long slot)
      throws SQLException {
    if (marked != (slot != null)) throw corrupt("retirement flag and proof disagree");
    if (!marked) return null;
    FixedRecords.Snapshot image =
        FixedRecords.read(connection, slot, FixedRecords.Kind.RETIREMENT, key(binding));
    if (image.header().revision() != 1
        || image.header().credits() != 0
        || image.header().capacity() != RetirementRecord.CAPACITY)
      throw corrupt("immutable retirement image geometry changed");
    RetirementRecord proof = RetirementRecord.decode(image.body());
    long minimum;
    try {
      minimum = Math.addExact(proof.root().closedAt(), binding.policy().receiptRetention());
    } catch (ArithmeticException invalid) {
      throw new SQLException("V2 retirement: creation receipt cutoff overflows", invalid);
    }
    if (!proof.context().equals(context(binding))
        || proof.creationSequence() != binding.creationSequence()
        || proof.cutoff() < minimum
        || proof.at() > AdmissionStore.watermark(connection, binding.authority())
        || !proof.root().equals(retainedRoot(connection, binding)))
      throw corrupt("retirement proof differs from retained authority or root");
    try (var query =
        connection.prepareStatement(
            "SELECT m.high_water,o.high_water FROM ps_v2_meta m JOIN ps_v2_owners o ON o.owner=?"
                + " WHERE m.singleton=1")) {
      query.setString(1, binding.owner());
      try (var row = query.executeQuery()) {
        if (!row.next()
            || row.getLong(1) < binding.generation()
            || row.getLong(2) < binding.creationSequence())
          throw corrupt("retirement non-reuse history contradicts binding");
      }
    }
    return proof;
  }

  private static ScopeSummary retainedRoot(Connection connection, Messages.Binding binding)
      throws SQLException {
    try {
      return DeclarationStore.scope(connection, binding, 0).state().summary();
    } catch (ProtocolError invalid) {
      // This root is mandatory durable evidence, not an optional peer-requested scope lookup.
      throw new SQLException("V2 retirement: retained root is missing or invalid", invalid);
    }
  }

  /**
   * Audit the complete live session before irreversible retirement; no partial-state exemptions.
   * Eligibility scans stream bounded records but may visit the entire named session.
   *
   * @param connection exclusive writer snapshot
   * @param config immutable deployment limits
   * @param binding exact session identity
   * @param revoked retained root revocation state
   * @param inputs paired payload installation
   * @param at current safe UTC sample
   * @return checked proof candidate, or null while any logical promise remains
   * @throws SQLException inconsistent closure, membership, resources or allocators
   * @throws IOException contradictory promised payload storage
   */
  static RetirementRecord eligible(
      Connection connection,
      SessionStore.Configuration config,
      Messages.Binding binding,
      boolean revoked,
      InputStore inputs,
      long at)
      throws SQLException, IOException {
    ScopeSummary root = DeclarationStore.scope(connection, binding, 0).state().summary();
    if (root == null) return null;
    long cutoff;
    try {
      cutoff = Math.addExact(root.closedAt(), binding.policy().receiptRetention());
    } catch (ArithmeticException overflow) {
      throw ProtocolError.limit("session retirement cutoff overflows");
    }
    if (at < cutoff) return null;
    DeclarationStore.audit(connection, binding);
    AdmissionStore.audit(connection, config, binding);
    FenceStore.auditScopes(connection, binding, revoked);
    ClosureStore.verify(connection, binding, 0);
    AdmissionStore.verifyStorage(connection, binding, inputs);
    try (var query =
        connection.prepareStatement(
            "SELECT scope,producer,id FROM ps_v2_entities WHERE generation=? ORDER BY scope,id")) {
      query.setLong(1, binding.generation());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          WorkKey work = new WorkKey(rows.getLong(1), rows.getInt(2), rows.getLong(3));
          WorkView view = DeclarationStore.member(connection, binding, work).view();
          if (!view.state().terminal()) throw corrupt("closed root contains unresolved work");
          cutoff = Math.max(cutoff, view.receiptUntil());
          if (view.outputUntil() != null) cutoff = Math.max(cutoff, view.outputUntil());
          AdmissionStore.StoredJob job = AdmissionStore.job(connection, binding, work);
          if (job != null
              && (job.record().inputLive()
                  || job.record().outputsLive()
                  || job.record().executorLive())) return null;
        }
      }
    }
    if (at < cutoff) return null;
    try (var query =
        connection.prepareStatement("SELECT id FROM ps_v2_scopes WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          ScopeSummary summary =
              DeclarationStore.scope(connection, binding, rows.getLong(1)).state().summary();
          if (summary == null || summary.closedAt() > cutoff)
            throw corrupt("retiring root contains an unresolved scope");
        }
      }
    }
    return new RetirementRecord(context(binding), binding.creationSequence(), root, cutoff, at);
  }

  /**
   * Recheck surviving terminal work and released jobs without relying on removed operations. The
   * original complete relational audit is retained by the immutable retirement proof.
   *
   * @param connection checked recovery snapshot
   * @param binding retained creation identity
   * @param proof already validated retirement authority
   * @throws SQLException surviving state contradicts retirement or its immutable identity
   */
  static void auditRemaining(
      Connection connection, Messages.Binding binding, RetirementRecord proof) throws SQLException {
    Objects.requireNonNull(proof);
    try (var query =
        connection.prepareStatement(
            "SELECT scope,producer,id,CASE WHEN length(declaration)=16 THEN declaration"
                + " END,view_slot,fence_slot FROM ps_v2_entities WHERE generation=? ORDER BY"
                + " scope,id")) {
      query.setLong(1, binding.generation());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          WorkKey work = new WorkKey(rows.getLong(1), rows.getInt(2), rows.getLong(3));
          byte[] declaration = rows.getBytes(4);
          if (declaration == null) throw corrupt("retiring member lost declaration identity");
          FixedRecords.Snapshot image =
              FixedRecords.read(
                  connection,
                  rows.getLong(5),
                  FixedRecords.Kind.WORK,
                  FixedRecords.key(
                      binding,
                      FixedRecords.Kind.WORK,
                      work.scope(),
                      work.producer(),
                      work.entity(),
                      declaration));
          FixedRecords.Snapshot fence =
              FixedRecords.read(
                  connection,
                  rows.getLong(6),
                  FixedRecords.Kind.FENCE,
                  FixedRecords.key(
                      binding,
                      FixedRecords.Kind.FENCE,
                      work.scope(),
                      work.producer(),
                      work.entity(),
                      declaration));
          FenceStore.decode(fence.body());
          WorkView view = view(image.body());
          if (!view.work().equals(work)) throw corrupt("retiring work identity differs");
          proof.verifyWork(view);
          AdmissionStore.StoredJob job = AdmissionStore.job(connection, binding, work);
          if (job != null) verifyJob(job.record(), proof);
        }
      }
    }
    try (var query =
        connection.prepareStatement("SELECT id FROM ps_v2_scopes WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var rows = query.executeQuery()) {
        while (rows.next()) {
          ScopeSummary summary =
              DeclarationStore.scope(connection, binding, rows.getLong(1)).state().summary();
          if (summary == null || summary.closedAt() > proof.cutoff())
            throw corrupt("retiring scope retains an unresolved promise");
        }
      }
    }
  }

  private static WorkView view(byte[] bytes) throws SQLException {
    try {
      Cbor.Reader in = new Cbor.Reader(bytes, Wire.MAX_CONTROL_LIMIT);
      WorkView view = RecordCodec.view(in);
      in.end();
      return view;
    } catch (ProtocolError invalid) {
      throw new SQLException("V2 retirement: invalid surviving work image", invalid);
    }
  }

  private static void verifyJob(JobRecord job, RetirementRecord proof) throws SQLException {
    if (job.inputLive()
        || job.outputsLive()
        || job.executorLive()
        || job.stage() != JobRecord.Stage.SETTLED
        || job.inputReleaseAt() == null
        || job.outputReleaseAt() == null
        || job.inputReleaseAt() > proof.at()
        || job.outputReleaseAt() > proof.at())
      throw corrupt("retiring job retains resources or lacks prior release evidence");
  }

  /**
   * Remove at most one metadata bundle inside a protected writer transaction. Foreign keys stay
   * enabled throughout; the root and immutable proof survive until the final session deletion.
   *
   * @param connection exclusive writer transaction
   * @param binding retained creation identity
   * @param proof checked immutable eligibility
   * @return the boundary that becomes durable only when the caller commits
   * @throws SQLException contradictory state or failed deletion
   */
  static Phase removeOne(Connection connection, Messages.Binding binding, RetirementRecord proof)
      throws SQLException {
    long generation = binding.generation();
    WorkKey jobWork = null;
    try (var query =
        connection.prepareStatement(
            "SELECT scope,producer,entity FROM ps_v2_jobs WHERE generation=? ORDER BY scope,entity"
                + " LIMIT 1")) {
      query.setLong(1, generation);
      try (var row = query.executeQuery()) {
        if (row.next()) jobWork = new WorkKey(row.getLong(1), row.getInt(2), row.getLong(3));
      }
    }
    if (jobWork != null) {
      AdmissionStore.StoredJob job = AdmissionStore.job(connection, binding, jobWork);
      if (job == null) throw corrupt("selected retirement job disappeared");
      verifyJob(job.record(), proof);
      remove(
          connection,
          "DELETE FROM ps_v2_jobs WHERE generation=? AND scope=? AND entity=?",
          generation,
          jobWork.scope(),
          jobWork.entity());
      removeSlot(connection, job.slot());
      return Phase.JOB_REMOVED;
    }
    if (removeOperation(connection, generation, false)) return Phase.OPERATION_REMOVED;
    long scope = -1, entity = 0, workSlot = 0, fenceSlot = 0;
    try (var query =
        connection.prepareStatement(
            "SELECT scope,id,view_slot,fence_slot FROM ps_v2_entities WHERE generation=? ORDER BY"
                + " scope,id LIMIT 1")) {
      query.setLong(1, generation);
      try (var row = query.executeQuery()) {
        if (row.next()) {
          scope = row.getLong(1);
          entity = row.getLong(2);
          workSlot = row.getLong(3);
          fenceSlot = row.getLong(4);
        }
      }
    }
    if (scope >= 0) {
      remove(
          connection,
          "DELETE FROM ps_v2_entities WHERE generation=? AND scope=? AND id=?",
          generation,
          scope,
          entity);
      removeSlot(connection, workSlot);
      removeSlot(connection, fenceSlot);
      return Phase.ENTITY_REMOVED;
    }
    if (removeOperation(connection, generation, true)) return Phase.OPERATION_REMOVED;
    long child = 0, childSlot = 0;
    try (var query =
        connection.prepareStatement(
            "SELECT id,state_slot FROM ps_v2_scopes WHERE generation=? AND id>0 ORDER BY id DESC"
                + " LIMIT 1")) {
      query.setLong(1, generation);
      try (var row = query.executeQuery()) {
        if (row.next()) {
          child = row.getLong(1);
          childSlot = row.getLong(2);
        }
      }
    }
    if (child > 0) {
      remove(connection, "DELETE FROM ps_v2_scopes WHERE generation=? AND id=?", generation, child);
      removeSlot(connection, childSlot);
      return Phase.SCOPE_REMOVED;
    }
    long rootSlot;
    long proofSlot;
    try (var query =
        connection.prepareStatement(
            "SELECT r.state_slot,s.retirement_slot FROM ps_v2_scopes r JOIN ps_v2_sessions s ON"
                + " r.generation=s.generation WHERE r.generation=? AND r.id=0 AND s.retiring=1")) {
      query.setLong(1, generation);
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("retirement final root or proof is missing");
        rootSlot = row.getLong(1);
        proofSlot = row.getLong(2);
      }
    }
    remove(connection, "DELETE FROM ps_v2_scopes WHERE generation=? AND id=0", generation);
    remove(connection, "DELETE FROM ps_v2_sessions WHERE generation=? AND retiring=1", generation);
    removeSlot(connection, rootSlot);
    removeSlot(connection, proofSlot);
    return Phase.FINISHED;
  }

  private static boolean removeOperation(
      Connection connection, long generation, boolean declaration) throws SQLException {
    int producer = 0;
    byte[] operation = null;
    try (var query =
        connection.prepareStatement(
            "SELECT producer,operation FROM ps_v2_operations WHERE generation=? AND request_kind"
                + (declaration ? "=0" : "!=0")
                + " ORDER BY producer,operation LIMIT 1")) {
      query.setLong(1, generation);
      try (var row = query.executeQuery()) {
        if (row.next()) {
          producer = row.getInt(1);
          operation = row.getBytes(2);
        }
      }
    }
    if (operation == null) return false;
    try (var delete =
        connection.prepareStatement(
            "DELETE FROM ps_v2_operations WHERE generation=? AND producer=? AND operation=?")) {
      delete.setLong(1, generation);
      delete.setInt(2, producer);
      delete.setBytes(3, operation);
      if (delete.executeUpdate() != 1) throw corrupt("retirement operation disappeared");
    }
    return true;
  }

  private static void removeSlot(Connection connection, long slot) throws SQLException {
    remove(connection, "DELETE FROM ps_v2_slots WHERE id=? AND kind!=0", slot);
  }

  private static void remove(Connection connection, String sql, long... parameters)
      throws SQLException {
    try (var delete = connection.prepareStatement(sql)) {
      for (int i = 0; i < parameters.length; i++) delete.setLong(i + 1, parameters[i]);
      if (delete.executeUpdate() != 1) throw corrupt("retirement bundle disappeared");
    }
  }

  /**
   * Derive the immutable session/creation ownership key for the private proof slot.
   *
   * @param binding checked creation receipt
   * @return fixed-record identity commitment
   */
  static byte[] key(Messages.Binding binding) {
    return FixedRecords.key(
        binding, FixedRecords.Kind.RETIREMENT, 0, 0, binding.creationSequence(), null);
  }

  /**
   * Extract the owner-qualified session identity used by physical storage.
   *
   * @param binding checked creation receipt
   * @return exact authority, owner and generation
   */
  static Commitments.Context context(Messages.Binding binding) {
    return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 retirement: " + detail);
  }
}
