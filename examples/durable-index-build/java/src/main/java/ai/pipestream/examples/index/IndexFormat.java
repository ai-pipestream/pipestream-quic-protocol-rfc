package ai.pipestream.examples.index;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Deterministic term-frequency and inverted-index formats, mirrored from
 * index-core (Rust). Byte layouts are the cross-implementation contract:
 * {@code term count} lines sorted byte-wise, and {@code term df doc:tf ...}
 * index lines with ascending numeric docs.
 */
public final class IndexFormat {
  /** Fixed chunk fan-out per file, matches index-core. */
  public static final int CHUNKS_PER_FILE = 4;

  private IndexFormat() {}

  /** Maximal runs of ASCII alphanumerics, lowercased. */
  public static List<String> terms(byte[] bytes) {
    List<String> terms = new ArrayList<>();
    byte[] current = new byte[64];
    int len = 0;
    for (byte raw : bytes) {
      int b = raw & 0xFF;
      boolean alnum =
          (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z');
      if (alnum) {
        if (b >= 'A' && b <= 'Z') b += 32;
        if (len == current.length) current = java.util.Arrays.copyOf(current, len * 2);
        current[len++] = (byte) b;
      } else if (len > 0) {
        terms.add(new String(current, 0, len, StandardCharsets.US_ASCII));
        len = 0;
      }
    }
    if (len > 0) terms.add(new String(current, 0, len, StandardCharsets.US_ASCII));
    return terms;
  }

  /** Term-frequency record bytes for one chunk. */
  public static byte[] tfRecord(byte[] chunk) {
    Map<String, Long> counts = new TreeMap<>();
    for (String term : terms(chunk)) counts.merge(term, 1L, Long::sum);
    StringBuilder out = new StringBuilder();
    for (Map.Entry<String, Long> e : counts.entrySet())
      out.append(e.getKey()).append(' ').append(e.getValue()).append('\n');
    return out.toString().getBytes(StandardCharsets.US_ASCII);
  }

  /** One parsed TF line. */
  public record TfEntry(String term, long count) {}

  /** Parse a TF record back into entries. */
  public static List<TfEntry> parseTf(byte[] record) {
    List<TfEntry> entries = new ArrayList<>();
    for (String line : new String(record, StandardCharsets.US_ASCII).split("\n")) {
      if (line.isEmpty()) continue;
      int space = line.indexOf(' ');
      entries.add(new TfEntry(line.substring(0, space), Long.parseLong(line.substring(space + 1))));
    }
    return entries;
  }

  /** All TF records of one document with its explicit document id. */
  public record DocRecord(long doc, List<byte[]> records) {}

  /**
   * Merge TF records into one inverted index. Doc ids are explicit (not
   * positions) so cancelled documents simply never appear.
   */
  public static byte[] mergeIndex(List<DocRecord> docs) {
    Map<String, Map<Long, Long>> postings = new TreeMap<>();
    for (DocRecord doc : docs) {
      Map<String, Long> docTerms = new TreeMap<>();
      for (byte[] record : doc.records())
        for (TfEntry e : parseTf(record)) docTerms.merge(e.term(), e.count(), Long::sum);
      for (Map.Entry<String, Long> e : docTerms.entrySet())
        postings.computeIfAbsent(e.getKey(), k -> new TreeMap<>()).put(doc.doc(), e.getValue());
    }
    StringBuilder out = new StringBuilder();
    for (Map.Entry<String, Map<Long, Long>> e : postings.entrySet()) {
      out.append(e.getKey()).append(' ').append(e.getValue().size());
      for (Map.Entry<Long, Long> posting : e.getValue().entrySet())
        out.append(' ').append(posting.getKey()).append(':').append(posting.getValue());
      out.append('\n');
    }
    return out.toString().getBytes(StandardCharsets.US_ASCII);
  }

  /** One parent-published reference: {@code doc scope producer entity digest}. */
  public record TfRef(long doc, long scope, int producer, long entity, byte[] digest) {}

  /** Format one reference line. */
  public static String formatRef(long doc, long scope, int producer, long entity, byte[] digest) {
    return doc + " " + scope + " " + producer + " " + entity + " " + HexFormat.of().formatHex(digest);
  }

  /** Parse reference lines (no header) into refs. */
  public static List<TfRef> parseRefs(String text) {
    List<TfRef> refs = new ArrayList<>();
    for (String line : text.split("\n")) {
      if (line.isEmpty()) continue;
      String[] parts = line.split(" ");
      if (parts.length != 5) throw new IllegalArgumentException("ref line needs 5 fields");
      byte[] digest = HexFormat.of().parseHex(parts[4]);
      if (digest.length != 32) throw new IllegalArgumentException("ref digest is 32 bytes");
      refs.add(
          new TfRef(
              Long.parseLong(parts[0]),
              Long.parseLong(parts[1]),
              Integer.parseInt(parts[2]),
              Long.parseLong(parts[3]),
              digest));
    }
    return refs;
  }

  /** SHA-256 hex digest of bytes. */
  public static String digestHex(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
