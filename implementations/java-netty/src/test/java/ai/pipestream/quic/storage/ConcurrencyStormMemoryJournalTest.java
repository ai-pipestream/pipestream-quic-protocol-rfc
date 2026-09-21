package ai.pipestream.quic.storage;

/** Runs the {@link ConcurrencyStormJournalTest} storm against the volatile in-memory journal. */
final class ConcurrencyStormMemoryJournalTest extends ConcurrencyStormJournalTest {
  @Override protected Journal openJournal() {
    return MemoryJournal.create();
  }
}
