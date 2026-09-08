package ai.pipestream.quic.v2;

import java.util.Objects;

/**
 * Test-only observation of real reached boundaries inside the durable endpoints. Production
 * launchers install {@link #NONE}. An implementation may record, pause at or halt after a boundary
 * the production code actually reached; it cannot forge commits, receipts, callbacks or results.
 * {@link #committed} runs on a host worker thread after the durable commit and may block; {@link
 * #sent} runs on a Netty event loop after native write acceptance and must not block.
 */
interface Boundaries {
  /** Named boundaries, shared with the fixture event schema. */
  enum Boundary {
    /** Listener bound. */
    LISTENING,
    /** Connection authenticated. */
    CONNECTION_AUTHENTICATED,
    /** Session creation committed. */
    SESSION_COMMITTED,
    /** Creation/attachment response accepted by the transport. */
    SESSION_RESPONSE_SENT,
    /** Declaration committed. */
    DECLARATION_COMMITTED,
    /** Declaration response accepted by the transport. */
    DECLARATION_RESPONSE_SENT,
    /** Input bytes installed, not yet admitted. */
    INPUT_INSTALLED,
    /** Admission committed. */
    ADMISSION_COMMITTED,
    /** Admission response accepted by the transport. */
    ADMISSION_RESPONSE_SENT,
    /** Explicit retry committed. */
    RETRY_COMMITTED,
    /** Cancellation, skip or scope-cancellation fence committed. */
    FENCE_COMMITTED,
    /** Result header accepted by the transport. */
    RESULT_HEADER_SENT,
    /** Result FIN accepted by the transport. */
    RESULT_FIN_SENT,
    /** Completed-session response accepted by the transport. */
    COMPLETE_RESPONSE_SENT,
    /** Detach acknowledgment accepted by the transport. */
    DETACH_ACKNOWLEDGED,
    /** A correlated refusal accepted by the transport. */
    REFUSAL_SENT,
    /** Listener drained its local owners. */
    SHUTDOWN_DRAINED
  }

  /**
   * Bounded identity carried with a boundary. Empty fields are absent, never invented.
   *
   * @param owner retained owner or empty
   * @param generation session generation or zero
   * @param operation operation identity or null
   * @param work logical work or null
   * @param attempt attempt or zero
   * @param refusal refusal code or null
   */
  record Details(
      String owner,
      long generation,
      Records.OperationId operation,
      Records.WorkKey work,
      long attempt,
      ProtocolError.Code refusal) {
    /** Empty details. */
    static final Details NONE = new Details("", 0, null, null, 0, null);

    /** Validate the owner label presence. */
    public Details {
      Objects.requireNonNull(owner);
    }

    Details owner(String value) {
      return new Details(value, generation, operation, work, attempt, refusal);
    }

    Details generation(long value) {
      return new Details(owner, value, operation, work, attempt, refusal);
    }

    Details operation(Records.OperationId value) {
      return new Details(owner, generation, value, work, attempt, refusal);
    }

    Details work(Records.WorkKey value) {
      return new Details(owner, generation, operation, value, attempt, refusal);
    }

    Details attempt(long value) {
      return new Details(owner, generation, operation, work, value, refusal);
    }

    Details refusal(ProtocolError.Code value) {
      return new Details(owner, generation, operation, work, attempt, value);
    }
  }

  /** Production observer: records nothing, pauses nothing, withholds nothing. */
  Boundaries NONE =
      new Boundaries() {
        @Override
        public void committed(Boundary boundary, Details details) {}

        @Override
        public void sent(Boundary boundary, Details details) {}

        @Override
        public boolean withhold(Boundary boundary) {
          return false;
        }
      };

  /**
   * A durable commit was reached on a worker thread. May block (fixture pause) or halt.
   *
   * @param boundary reached boundary
   * @param details bounded identity
   */
  void committed(Boundary boundary, Details details);

  /**
   * A transport write was accepted natively, on the event loop. Must not block.
   *
   * @param boundary reached boundary
   * @param details bounded identity
   */
  void sent(Boundary boundary, Details details);

  /**
   * Whether the fixture asks the endpoint to drop the reply for a committed boundary and close the
   * connection instead, producing a real lost-ACK. Consulted on the event loop; must not block.
   *
   * @param boundary the response boundary about to be reached
   * @return true to withhold the reply
   */
  boolean withhold(Boundary boundary);
}
