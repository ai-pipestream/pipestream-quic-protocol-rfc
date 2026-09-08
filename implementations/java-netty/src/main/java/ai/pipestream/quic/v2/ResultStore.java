package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.Records.*;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;

/**
 * Exact retained result evidence and physical reads; neither locators nor worker IDs authorize
 * access.
 */
final class ResultStore {
  /** Current local result permission, separate from permission to execute the application. */
  @FunctionalInterface
  interface Authorization {
    /**
     * Recheck permission before inspecting result contents.
     *
     * @param binding authenticated retained session
     * @param work requested logical work
     */
    void check(Binding binding, WorkKey work);
  }

  /**
   * One already pinned object. The delivery service must enforce authorization and elapsed
   * deadlines.
   *
   * @param header exact committed response identity
   * @param reader verified payload-only descriptor, closed on abort or FIN
   */
  record Opened(ResultHeader header, InputStream reader) implements AutoCloseable {
    /** Require the committed header and live descriptor. */
    Opened {
      Objects.requireNonNull(header);
      Objects.requireNonNull(reader);
    }

    /**
     * Release the physical pin, without refunding the durable object allowance.
     *
     * @throws IOException failed physical close; the allowance must remain charged
     */
    @Override
    public void close() throws IOException {
      reader.close();
    }
  }

  private ResultStore() {}

  /**
   * Return the audited manifest retained for one producing work attempt.
   *
   * @param connection current session database connection
   * @param binding authorized session binding
   * @param work producing work key
   * @param attempt producing attempt number
   * @return retained published manifest
   * @throws SQLException retained state cannot be read or audited
   */
  static Manifest retained(Connection connection, Binding binding, WorkKey work, long attempt)
      throws SQLException {
    WorkView view = DeclarationStore.member(connection, binding, work).view();
    if (view.attempt() != attempt)
      throw error(ProtocolError.Code.NOT_FOUND, "producing attempt not retained");
    Manifest manifest = view.manifest();
    if (manifest == null) throw error(ProtocolError.Code.NOT_READY, "result not published");
    AdmissionStore.StoredJob stored = AdmissionStore.job(connection, binding, work);
    if (stored == null) throw new SQLException("V2 result: published job missing");
    JobRecord job = stored.record();
    if (job.stage() != JobRecord.Stage.SETTLED
        || job.attempt() != attempt
        || job.lease() == 0
        || !job.input().parameters().work().equals(work)
        || !job.input().parameters().input().equals(view.input()))
      throw new SQLException("V2 result: publication contradicts producing job");
    PublicationStore.audit(connection, binding, view, job);
    return manifest;
  }

  /**
   * Resolve and commitment-check one requested output in a manifest.
   *
   * @param manifest retained result manifest
   * @param request client result read request
   * @return requested committed output
   */
  static Output requested(Manifest manifest, Read request) {
    if (request.index() >= manifest.outputs().size())
      throw error(ProtocolError.Code.NOT_FOUND, "output index not published");
    Output output = manifest.outputs().get(request.index());
    if (!output.sha256().equals(request.expectedSha256()))
      throw error(ProtocolError.Code.INTEGRITY_ERROR, "requested output commitment differs");
    return output;
  }

  /**
   * Refuse reads after the manifest's availability deadline.
   *
   * @param manifest retained result manifest
   * @param now current UTC sample
   */
  static void available(Manifest manifest, long now) {
    if (now >= manifest.availableUntil())
      throw error(ProtocolError.Code.EXPIRED, "output availability expired");
  }

  /**
   * Open a verified reader for a retained published output.
   *
   * @param connection current session database connection
   * @param installation local installation identifier
   * @param binding authorized session binding
   * @param inputs local object store
   * @param request client result read request
   * @param output verified output commitment
   * @return result header and verified reader
   * @throws SQLException retained state cannot be read
   */
  static Opened open(
      Connection connection,
      UUID installation,
      Binding binding,
      InputStore inputs,
      Read request,
      Output output)
      throws SQLException {
    AdmissionStore.StoredJob stored = AdmissionStore.job(connection, binding, request.work());
    if (stored == null) throw new SQLException("V2 result: published job missing");
    JobRecord job = stored.record();
    if (!job.outputsLive() || job.releaseIntent() == 2)
      throw error(ProtocolError.Code.OUTPUT_UNAVAILABLE, "promised output has been reclaimed");
    ExecutionStore.Lease producer =
        new ExecutionStore.Lease(
            installation,
            binding.owner(),
            binding.generation(),
            request.work(),
            request.attempt(),
            job.lease(),
            1);
    ResultHeader header =
        new ResultHeader(
            request.request(),
            binding.generation(),
            request.work(),
            request.attempt(),
            request.index(),
            output.length(),
            output.sha256());
    Wire.encodeRecord(header, Wire.HEADER_LIMIT);
    try {
      InputStream reader =
          inputs.openPublishedOutput(
              new Commitments.Context(binding.authority(), binding.owner(), binding.generation()),
              job.input(),
              producer,
              output);
      return new Opened(header, reader);
    } catch (IOException failure) {
      ProtocolError refusal =
          error(ProtocolError.Code.OUTPUT_UNAVAILABLE, "retained output I/O failed");
      refusal.initCause(failure);
      throw refusal;
    } catch (ProtocolError failure) {
      if (failure.code() == ProtocolError.Code.LIMIT_EXCEEDED) throw failure;
      ProtocolError refusal =
          error(ProtocolError.Code.OUTPUT_UNAVAILABLE, "retained output differs");
      refusal.initCause(failure);
      throw refusal;
    }
  }

  private static ProtocolError error(ProtocolError.Code code, String detail) {
    return new ProtocolError(code, detail);
  }
}
