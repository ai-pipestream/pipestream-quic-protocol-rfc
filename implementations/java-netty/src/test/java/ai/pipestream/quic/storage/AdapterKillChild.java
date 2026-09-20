package ai.pipestream.quic.storage;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * Subprocess entry point for the adapter's kill-recovery test: journals a
 * fixed sequence through {@link ClientJournalAdapter}, announcing each
 * durable append on stdout, then either closes cleanly or waits to be killed
 * by the parent process, leaving the SQLite rollback journal to prove
 * recovery on the next open.
 */
public final class AdapterKillChild {
  private AdapterKillChild() {}

  /**
   * Journals intents kill-1..kill-N with a commit on every even index.
   * @param args {@code <file> <intents> <cleanClose:true|false>}
   */
  public static void main(String[] args) throws Exception {
    Path file = Path.of(args[0]);
    int intents = Integer.parseInt(args[1]);
    boolean cleanClose = Boolean.parseBoolean(args[2]);
    ClientJournalAdapter journal = ClientJournalAdapter.initialize(file);
    for (int i = 1; i <= intents; i++) {
      journal.appendIntent(new Journal.Intent("kill-" + i,
          ("payload-" + i).getBytes(StandardCharsets.UTF_8)));
      if (i % 2 == 0) {
        journal.commit("kill-" + i, ("commit-" + i).getBytes(StandardCharsets.UTF_8));
      }
      System.out.println("journaled " + i);
      System.out.flush();
    }
    if (cleanClose) {
      journal.close();
    } else {
      // Abandon the journal and wait for the parent to kill this process:
      // the file must recover through ClientJournal's own open path.
      Thread.sleep(120_000);
    }
  }
}
