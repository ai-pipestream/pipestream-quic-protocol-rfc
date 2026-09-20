package ai.pipestream.quic.storage;

/** Certifies {@link MemoryJournal} against the journal contract. */
final class MemoryJournalContractTest extends JournalContractTest {
  @Override protected Journal openJournal() {
    return MemoryJournal.create();
  }
}
