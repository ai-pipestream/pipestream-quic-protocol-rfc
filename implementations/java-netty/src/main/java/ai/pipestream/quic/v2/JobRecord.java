package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Records.*;

import java.sql.SQLException;
import java.util.Objects;

/**
 * One bounded, restartable job and its live retention charges. This is a private Java storage
 * record, not a wire message. Leases are local execution ownership, separate from wire attempts.
 *
 * @param input immutable admission intent
 * @param safety configured safe-restart contract
 * @param attempt current wire attempt
 * @param lease non-reusable local execution ownership counter, zero before acquisition
 * @param leaseUntil current lease expiry, null without an active lease
 * @param stage current scheduler state
 * @param inputReference immutable input name within the paired store
 * @param outputReference immutable output-funding name within the paired store
 * @param objectLimit retained maximum individual output size
 * @param inputLive input retention remains charged
 * @param outputsLive output funding remains charged
 * @param executorLive executor capacity remains charged
 * @param expansionComplete authority expansion finished, independently of its membership seal
 * @param inputReleaseAt durable input-deletion eligibility time, retained after refund
 * @param outputReleaseAt durable output-deletion eligibility time, retained after refund
 */
record JobRecord(
    InputHeader input,
    AdmissionStore.RestartSafety safety,
    long attempt,
    long lease,
    Long leaseUntil,
    Stage stage,
    String inputReference,
    String outputReference,
    long objectLimit,
    boolean inputLive,
    boolean outputsLive,
    boolean executorLive,
    boolean expansionComplete,
    Long inputReleaseAt,
    Long outputReleaseAt) {
  /** Durable scheduling states, independent of client observation and stream lifetime. */
  enum Stage {
    /** Admitted and eligible for lease acquisition. */
    QUEUED,
    /** A current lease owns execution. */
    EXECUTING,
    /** Branch awaits verified child closure. */
    WAITING_CHILDREN,
    /** An explicit authorized retry is required. */
    AWAITING_RETRY,
    /** The logical job has an authoritative terminal outcome. */
    SETTLED,
    /** An accepted own fence excludes execution while descendants finish settlement. */
    CANCELLING
  }

  /** Validate bounded identity and state; no generic object tree is retained. */
  JobRecord {
    Objects.requireNonNull(input);
    Objects.requireNonNull(safety);
    Objects.requireNonNull(stage);
    Checks.id(attempt);
    Checks.number(lease);
    Checks.number(objectLimit);
    ProtocolError.require(
        objectLimit <= input.parameters().outputs().totalBytes(),
        "individual output ceiling exceeds its funded total");
    if (inputReleaseAt != null) Checks.number(inputReleaseAt);
    if (outputReleaseAt != null) Checks.number(outputReleaseAt);
    ProtocolError.require(
        inputReference != null && inputReference.matches("[0-9a-f]{64}\\.input"),
        "invalid retained input reference");
    ProtocolError.require(
        outputReference != null && outputReference.matches("[0-9a-f]{64}\\.funding"),
        "invalid retained output funding reference");
    if (leaseUntil != null) Checks.number(leaseUntil);
    ProtocolError.require(leaseUntil == null || lease > 0, "lease deadline without lease identity");
    ProtocolError.require(
        (stage == Stage.EXECUTING) == (leaseUntil != null), "job lease state differs");
    ProtocolError.require(
        stage == Stage.SETTLED || inputLive && outputsLive && executorLive,
        "unsettled job lost funded resources");
    ProtocolError.require(
        stage != Stage.SETTLED || !executorLive, "settled job holds executor slot");
    ProtocolError.require(
        stage != Stage.WAITING_CHILDREN || input.parameters().mode() != 0,
        "leaf job waits for children");
    ProtocolError.require(
        input.parameters().mode() == 2 || expansionComplete,
        "non-expanding job has expansion obligation");
    ProtocolError.require(
        inputReleaseAt == null && outputReleaseAt == null || stage == Stage.SETTLED,
        "unsettled job has reclamation evidence");
    ProtocolError.require(inputLive || inputReleaseAt != null, "input refund without evidence");
    ProtocolError.require(outputsLive || outputReleaseAt != null, "output refund without evidence");
  }

  /**
   * Encode one job within the capacity funded at admission.
   *
   * @return canonical, bounded private record
   */
  byte[] encode() {
    Cbor.Writer out = new Cbor.Writer(FixedRecords.JOB_CAPACITY);
    out.array(15);
    RecordCodec.write(out, input);
    out.number(safety.ordinal());
    out.number(attempt);
    out.number(lease);
    if (leaseUntil == null) out.nil();
    else out.number(leaseUntil);
    out.number(stage.ordinal());
    out.text(inputReference, 128);
    out.text(outputReference, 128);
    out.number(objectLimit);
    out.bool(inputLive);
    out.bool(outputsLive);
    out.bool(executorLive);
    out.bool(expansionComplete);
    if (inputReleaseAt == null) out.nil();
    else out.number(inputReleaseAt);
    if (outputReleaseAt == null) out.nil();
    else out.number(outputReleaseAt);
    return out.finish();
  }

  /**
   * Decode one job, rejecting unsupported local state instead of adopting it.
   *
   * @param bytes checked fixed-record body
   * @return typed restart and retention state
   * @throws SQLException corrupt local encoding
   */
  static JobRecord decode(byte[] bytes) throws SQLException {
    try {
      Cbor.Reader in = new Cbor.Reader(bytes, FixedRecords.JOB_CAPACITY);
      in.exact(15);
      InputHeader input = RecordCodec.inputHeader(in);
      int safety =
          (int) Checks.range(in.number(), 0, AdmissionStore.RestartSafety.values().length - 1);
      long attempt = in.number(), lease = in.number();
      Long leaseUntil = in.nullable() ? null : in.number();
      int stage = (int) Checks.range(in.number(), 0, Stage.values().length - 1);
      JobRecord result =
          new JobRecord(
              input,
              AdmissionStore.RestartSafety.values()[safety],
              attempt,
              lease,
              leaseUntil,
              Stage.values()[stage],
              in.text(128),
              in.text(128),
              in.number(),
              in.bool(),
              in.bool(),
              in.bool(),
              in.bool(),
              in.nullable() ? null : in.number(),
              in.nullable() ? null : in.number());
      in.end();
      return result;
    } catch (ProtocolError invalid) {
      throw new SQLException("V2 job record: invalid encoding", invalid);
    }
  }
}
