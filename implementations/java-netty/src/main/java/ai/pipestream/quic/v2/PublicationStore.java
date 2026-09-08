package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Checked immutable output descriptors for one fenced success commit. Files are installed before
 * this metadata operation; neither an installed file nor a returned descriptor publishes success.
 */
final class PublicationStore {
  /**
   * Trusted deployment endpoint, never an application callback's URL or a caller credential.
   *
   * @param authority DNS name or bracketed IPv6 address and explicit port
   */
  record Endpoint(String authority) {
    /** Validate syntax without network access, redirects or authority discovery. */
    Endpoint {
      Objects.requireNonNull(authority);
      locator(authority, 1, new WorkKey(0, 0, 1), 1, 0);
    }

    private Locator locator(long generation, WorkKey work, long attempt, int index) {
      return locator(authority, generation, work, attempt, index);
    }

    private static Locator locator(
        String authority, long generation, WorkKey work, long attempt, int index) {
      return new Locator(
          "pipestream://"
              + authority
              + "/v2/sessions/"
              + generation
              + "/scopes/"
              + work.scope()
              + "/producers/"
              + work.producer()
              + "/entities/"
              + work.entity()
              + "/attempts/"
              + attempt
              + "/outputs/"
              + index);
    }
  }

  /**
   * Bounded callback completion intent, not an authoritative outcome.
   *
   * @param count exact number of finished output objects
   * @param endpoint trusted authority endpoint
   */
  record Request(int count, Endpoint endpoint) {
    /** Validate local syntax; retained admission budgets are checked in the transaction. */
    Request {
      Checks.range(count, 0, 256);
      Objects.requireNonNull(endpoint);
    }
  }

  private PublicationStore() {}

  /**
   * Verify the exact completed output set on the paired store before sampling publication time. The
   * caller holds both the input-store monitor and the authoritative metadata transaction.
   *
   * @param inputs exclusive paired input/output store
   * @param binding retained session identity
   * @param loaded current work and job
   * @param lease current worker fence
   * @param results retained result-delivery selection
   * @param request completion intent
   * @return bounded contiguous verified descriptors, never payload-sized data
   * @throws IOException missing, unfinished, corrupt or unsynchronized storage
   */
  static List<Output> prepare(
      InputStore inputs,
      Messages.Binding binding,
      ExecutionStore.Loaded loaded,
      ExecutionStore.Lease lease,
      boolean results,
      Request request)
      throws IOException {
    JobRecord job = loaded.stored().record();
    OutputBudget budget = job.input().parameters().outputs();
    if (request.count() > budget.count() || !results && request.count() != 0)
      throw ProtocolError.limit("success output count exceeds its admitted profile or budget");
    Commitments.Context context =
        new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
    inputs.verifyOutputCount(context, job.input(), lease, request.count());
    List<Output> outputs = new ArrayList<>(request.count());
    long total = 0;
    for (int index = 0; index < request.count(); index++) {
      OutputStore.Stored stored =
          inputs
              .findOutput(context, job.input(), lease, index)
              .orElseThrow(() -> new IOException("success output is missing"));
      if (stored.length() > job.objectLimit() || stored.length() > budget.totalBytes() - total)
        throw ProtocolError.limit("success output bytes exceed the admitted budget");
      total += stored.length();
      outputs.add(
          new Output(
              index,
              stored.length(),
              stored.sha256(),
              stored.contentType(),
              request
                  .endpoint()
                  .locator(
                      binding.generation(), loaded.entity().view().work(), job.attempt(), index)));
    }
    return List.copyOf(outputs);
  }

  /**
   * Check published descriptors and retention promises without inventing storage availability.
   *
   * @param connection stable metadata snapshot
   * @param binding retained session
   * @param view successful retained work
   * @param job exact admitted job
   * @throws SQLException contradictory manifest, profile or budget
   */
  static void audit(Connection connection, Messages.Binding binding, WorkView view, JobRecord job)
      throws SQLException {
    try {
      boolean results = results(connection, binding);
      view.validateProfiles(results);
      Manifest manifest = view.manifest();
      if (manifest == null) {
        if (job.input().parameters().outputs().count() != 0
            || job.input().parameters().outputs().totalBytes() != 0)
          throw corrupt("result-disabled success retains a nonzero output budget");
        return;
      }
      if (!manifest.authority().equals(binding.authority())
          || !manifest.owner().equals(binding.owner())
          || manifest.generation() != binding.generation()
          || manifest.availableUntil() != add(view.terminalAt(), binding.policy().outputRetention())
          || manifest.outputs().size() > job.input().parameters().outputs().count())
        throw corrupt("manifest identity, interval or count contradicts admission");
      long total = 0;
      for (Output output : manifest.outputs()) {
        if (output.length() > job.objectLimit()
            || output.length() > job.input().parameters().outputs().totalBytes() - total)
          throw corrupt("manifest bytes exceed admitted allowance");
        total += output.length();
      }
    } catch (ProtocolError invalid) {
      throw new SQLException("V2 publication: invalid retained success", invalid);
    }
  }

  /**
   * Verify all retained published bytes during paired-store recovery, not merely their filenames.
   *
   * @param binding retained session
   * @param inputs paired input/output store
   * @param view successful retained work
   * @param job exact settled job
   * @throws IOException missing or corrupt published bytes
   */
  static void verifyStorage(
      Messages.Binding binding, InputStore inputs, WorkView view, JobRecord job)
      throws IOException {
    if (view.manifest() == null || !job.outputsLive()) return;
    Commitments.Context context =
        new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
    // Lease expiry is no longer live in SETTLED. Only its immutable acquisition identity locates
    // the installed files; the synthetic observation here grants no execution or read permission.
    ExecutionStore.Lease lease =
        new ExecutionStore.Lease(
            inputs.authorityIdentity().orElseThrow(),
            binding.owner(),
            binding.generation(),
            view.work(),
            job.attempt(),
            job.lease(),
            1);
    inputs.verifyOutputCount(context, job.input(), lease, view.manifest().outputs().size());
    for (Output expected : view.manifest().outputs()) {
      OutputStore.Stored actual =
          inputs
              .findOutput(context, job.input(), lease, expected.index())
              .orElseThrow(() -> new IOException("published output is missing"));
      if (expected.length() != actual.length()
          || !expected.sha256().equals(actual.sha256())
          || !expected.contentType().equals(actual.contentType()))
        throw new IOException("published output contradicts its retained manifest");
    }
  }

  private static boolean results(Connection connection, Messages.Binding binding)
      throws SQLException {
    try (var query =
        connection.prepareStatement("SELECT profiles FROM ps_v2_sessions WHERE generation=?")) {
      query.setLong(1, binding.generation());
      try (var row = query.executeQuery()) {
        if (!row.next()) throw corrupt("session profile binding missing");
        int profiles = row.getInt(1);
        if (profiles != 1 && profiles != 3) throw corrupt("invalid retained profiles");
        return profiles == 3;
      }
    }
  }

  private static long add(long left, long right) {
    if (left < 0 || right < 0 || right > Long.MAX_VALUE - left)
      throw ProtocolError.limit("publication timestamp exhausted");
    return left + right;
  }

  private static SQLException corrupt(String detail) {
    return new SQLException("V2 publication: " + detail);
  }
}
