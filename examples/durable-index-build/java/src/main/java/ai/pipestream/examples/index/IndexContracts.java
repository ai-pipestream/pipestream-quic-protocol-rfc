package ai.pipestream.examples.index;

import ai.pipestream.quic.v2.ClientJournal;
import ai.pipestream.quic.v2.ClientOptions;
import ai.pipestream.quic.v2.DurableClient;
import ai.pipestream.quic.v2.DurableHost;
import ai.pipestream.quic.v2.Records;
import ai.pipestream.quic.v2.ResultFiles;
import ai.pipestream.quic.v2.TlsAuthentication;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * The three index-build application contracts, mirrored from the Rust
 * authority: {@code index-file/v1} (mode 2, authority-side expansion into
 * {@code tf/v1} children, then the child reference list), {@code tf/v1}
 * (mode 0, term-frequency record), {@code index-merge/v1} (mode 0, merge TF
 * records fetched by authenticated result reference).
 */
public final class IndexContracts {
  static final String FILE_LABEL = "index-file/v1";
  static final String TF_LABEL = "tf/v1";
  static final String MERGE_LABEL = "index-merge/v1";

  /** Wire integrity failure code (matches the Rust ErrorCode numbering). */
  static final long INTEGRITY_ERROR = 8;
  /** Internal failure code. */
  static final long INTERNAL_ERROR = 15;

  private IndexContracts() {}

  /** Stable operation identity from a domain-separated SHA-256. */
  static Records.OperationId operation(String domain, long a, long b) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      sha.update(domain.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      ByteBuffer longs = ByteBuffer.allocate(16);
      longs.putLong(a).putLong(b);
      sha.update(longs.array());
      byte[] digest = sha.digest();
      byte[] id = java.util.Arrays.copyOf(digest, 16);
      if (isZero(id)) id[15] = 1;
      return new Records.OperationId(id);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static boolean isZero(byte[] bytes) {
    for (byte b : bytes) if (b != 0) return false;
    return true;
  }

  static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /** Read a whole input stream (bounded by the commitment). */
  static byte[] readAll(DurableHost.Work work) throws Exception {
    java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
    byte[] chunk = new byte[Math.min(work.bufferLimit(), 8192)];
    for (; ; ) {
      int n = work.readInput(chunk, 0, chunk.length);
      if (n < 0) break;
      out.write(chunk, 0, n);
      work.renew();
    }
    return out.toByteArray();
  }

  static void writeAll(DurableHost.Work work, byte[] bytes) throws Exception {
    int offset = 0;
    while (offset < bytes.length) {
      int n = Math.min(work.bufferLimit(), bytes.length - offset);
      work.writeOutput(ByteBuffer.wrap(bytes, offset, n));
      offset += n;
    }
  }

  /** index-file/v1 expansion: split the file into fixed chunks, declare the
   * sealed child scope, admit one tf/v1 leaf per chunk. */
  static DurableHost.Expansion expandFile(DurableHost.Production production) throws Exception {
    long total = production.input().length();
    long per = total / IndexFormat.CHUNKS_PER_FILE;
    List<Long> members = new ArrayList<>();
    for (long i = 1; i <= IndexFormat.CHUNKS_PER_FILE; i++) members.add(i);
    production.declare(operation("index-file-declare", total, 0), members, true);
    byte[] staging = new byte[Math.max(1, (int) Math.min(production.bufferLimit(), per))];
    long consumed = 0;
    for (long entity = 1; entity <= IndexFormat.CHUNKS_PER_FILE; entity++) {
      production.renew();
      long end = entity == IndexFormat.CHUNKS_PER_FILE ? total : entity * per;
      int expected = (int) (end - consumed);
      byte[] chunk = new byte[expected];
      int filled = 0;
      while (filled < expected) {
        int n =
            production.readInput(
                staging, 0, Math.min(staging.length, expected - filled));
        if (n < 0)
          return DurableHost.Expansion.failed(
              INTEGRITY_ERROR, "file input ended before its commitment");
        System.arraycopy(staging, 0, chunk, filled, n);
        filled += n;
      }
      consumed = end;
      Records.AdmitParameters parameters =
          new Records.AdmitParameters(
              new Records.WorkKey(production.childScope(), 1, entity),
              new Records.Input(expected, new Records.Digest(sha256(chunk)), "text/corpus"),
              TF_LABEL,
              0,
              production.executionMs(),
              new Records.OutputBudget(1, expected * 2L + 64));
      if (production.beginInput(operation("index-file-admit", total, entity), parameters).isEmpty()) {
        for (int offset = 0; offset < chunk.length; ) {
          int n = Math.min(production.bufferLimit(), chunk.length - offset);
          production.writeInput(ByteBuffer.wrap(chunk, offset, n));
          offset += n;
        }
        production.finishInput();
      }
    }
    if (production.readInput(staging, 0, 1) >= 0)
      return DurableHost.Expansion.failed(INTEGRITY_ERROR, "file input exceeds its commitment");
    return DurableHost.Expansion.complete();
  }

  /** index-file/v1 execute: read every child TF output (verified EOF) and
   * publish the reference list. TF bytes stay on this authority. */
  static DurableHost.Result runFile(DurableHost.Work work) throws Exception {
    long doc = work.key().entity() - 1;
    List<String> refs = new ArrayList<>();
    long after = 0;
    for (; ; ) {
      DurableHost.ChildPage page = work.children(after, 256);
      if (page.members().isEmpty()) break;
      for (Records.WorkKey member : page.members()) {
        Records.Output output = work.beginChildOutput(member.entity(), 0);
        java.io.ByteArrayOutputStream record = new java.io.ByteArrayOutputStream();
        byte[] chunk = new byte[Math.min(work.bufferLimit(), 8192)];
        for (; ; ) {
          int n = work.readChildOutput(chunk, 0, chunk.length);
          if (n < 0) break;
          record.write(chunk, 0, n);
          work.renew();
        }
        work.finishChildOutput();
        byte[] digest = sha256(record.toByteArray());
        if (!MessageDigest.isEqual(digest, output.sha256().bytes()))
          return DurableHost.Result.failed(
              INTEGRITY_ERROR, "child TF record digest differs from manifest");
        refs.add(
            IndexFormat.formatRef(doc, member.scope(), member.producer(), member.entity(), digest));
        after = member.entity();
        work.renew();
      }
      if (!page.more()) break;
    }
    byte[] list = (String.join("\n", refs) + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
    work.beginOutput(list.length, "text/refs");
    writeAll(work, list);
    work.finishOutput();
    return DurableHost.Result.succeeded();
  }

  /** tf/v1 execute: deterministic term-frequency record over one chunk. */
  static DurableHost.Result runTf(DurableHost.Work work) throws Exception {
    byte[] record = IndexFormat.tfRecord(readAll(work));
    work.beginOutput(record.length, "text/tf");
    writeAll(work, record);
    work.finishOutput();
    return DurableHost.Result.succeeded();
  }

  /** Reader configuration: separately configured owner credentials plus the
   * peer endpoint (flags, never URIs or unit-input bytes). */
  public record ReaderConfig(
      InetSocketAddress endpoint,
      String serverName,
      Path ca,
      Path cert,
      Path key,
      String owner) {}

  /** Split merge input into its header line and reference body. */
  static String[] splitMergeInput(byte[] input) {
    String text = new String(input, java.nio.charset.StandardCharsets.UTF_8);
    int newline = text.indexOf('\n');
    if (newline < 0) throw new IllegalArgumentException("merge input needs a header");
    return new String[] {text.substring(0, newline), text.substring(newline + 1)};
  }

  /** Merge input header: {@code v1 authority=<a> creation=<c>}. */
  static String[] parseHeader(String header) {
    String authority = null;
    long creation = -1;
    for (String part : header.split(" ")) {
      if (part.startsWith("authority=")) authority = part.substring("authority=".length());
      if (part.startsWith("creation=")) creation = Long.parseLong(part.substring("creation=".length()));
    }
    if (authority == null || creation < 0) throw new IllegalArgumentException("merge header incomplete");
    return new String[] {authority, Long.toString(creation)};
  }

  static <T> T get(java.util.concurrent.CompletionStage<T> stage) throws Exception {
    try {
      return stage.toCompletableFuture().get(120, TimeUnit.SECONDS);
    } catch (java.util.concurrent.ExecutionException | java.util.concurrent.CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception exception) throw exception;
      throw e;
    }
  }

  /**
   * Read one TF record through an owner session on the peer authority:
   * attach under the producing session's creation identity, watch to
   * terminal, manifest plus select plus read output 0, verify the digest.
   * Read-only: the fresh journal never declares or admits.
   */
  static byte[] readReference(DurableClient client, IndexFormat.TfRef ref, Path scratch)
      throws Exception {
    Path dir = scratch.resolve("ref-" + ref.scope() + "-" + ref.producer() + "-" + ref.entity());
    deleteTree(dir);
    Files.createDirectories(dir);
    Records.WorkKey key = new Records.WorkKey(ref.scope(), ref.producer(), ref.entity());
    long after = 0;
    long attempt = -1;
    for (; ; ) {
      ClientJournal.Observed observed = get(client.watch(key, after, 10_000));
      after = observed.revision();
      long state = observed.view().state().value();
      if (state >= 5 && state <= 8) {
        if (state != 5) throw new IllegalStateException("TF child terminal state " + state);
        attempt = observed.view().attempt();
        break;
      }
    }
    get(client.manifest(key, attempt));
    get(client.select(key, attempt, 0));
    Path out = dir.resolve("tf.bin");
    ResultFiles.Delivered delivered =
        get(client.read(key, attempt, 0, new ResultFiles.Destination(out)));
    byte[] bytes = Files.readAllBytes(out);
    if (bytes.length != delivered.length()
        || !MessageDigest.isEqual(sha256(bytes), ref.digest()))
      throw new IllegalStateException("TF record digest differs from the parent-published reference");
    return bytes;
  }

  /** index-merge/v1 execute factory: needs the separately configured reader. */
  static DurableHost.Processor runMerge(ReaderConfig reader) {
    return work -> {
      if (reader == null)
        return DurableHost.Result.failed(INTERNAL_ERROR, "merge reader not configured on this authority");
      byte[] input = readAll(work);
      String[] header = splitMergeInput(input);
      String[] parsed = parseHeader(header[0]);
      String authority = parsed[0];
      long creation = Long.parseLong(parsed[1]);
      List<IndexFormat.TfRef> refs;
      try {
        refs = IndexFormat.parseRefs(header[1]);
      } catch (IllegalArgumentException e) {
        return DurableHost.Result.failed(INTERNAL_ERROR, "merge refs unreadable: " + e.getMessage());
      }
      Path scratch;
      try {
        scratch = Files.createTempDirectory("index-merge-reader-");
      } catch (Exception e) {
        return DurableHost.Result.failed(INTERNAL_ERROR, "reader scratch failed: " + e);
      }
      // One reader session for the whole merge: connect once, read every
      // TF reference over the same client, detach once. A connect per
      // reference churns server connections (default ceiling is 16).
      try {
        Map<Long, List<byte[]>> docs = new TreeMap<>();
        Path sessionDir = scratch.resolve("session");
        deleteTree(sessionDir);
        Files.createDirectories(sessionDir);
        ClientJournal.Intent intent =
            new ClientJournal.Intent(
                authority,
                reader.owner(),
                creation,
                new Records.Policy(60_000, 3_600_000, 86_400_000),
                true);
        try (ClientJournal journal =
                ClientJournal.initialize(
                    sessionDir.resolve("reader.sqlite"), intent, ClientJournal.Limits.defaults());
            DurableClient client =
                DurableClient.connect(
                    reader.endpoint(),
                    TlsAuthentication.client(
                        reader.ca(), reader.serverName(), reader.cert(), reader.key()),
                    journal,
                    ClientOptions.defaults())) {
          get(client.ready());
          get(client.binding());
          for (IndexFormat.TfRef ref : refs) {
            byte[] record;
            try {
              record = readReference(client, ref, scratch);
            } catch (Exception e) {
              System.err.println("index-merge consumer read failed: " + e);
              try {
                get(client.detach());
              } catch (Exception detached) {
                System.err.println("index-merge reader detach after failure failed: " + detached);
              }
              return DurableHost.Result.failed(
                  INTERNAL_ERROR, "consumer read failed: " + trim(e.toString()));
            }
            docs.computeIfAbsent(ref.doc(), k -> new ArrayList<>()).add(record);
            work.renew();
          }
          get(client.detach());
        }
        List<IndexFormat.DocRecord> ordered = new ArrayList<>();
        for (Map.Entry<Long, List<byte[]>> e : docs.entrySet())
          ordered.add(new IndexFormat.DocRecord(e.getKey(), e.getValue()));
        byte[] merged = IndexFormat.mergeIndex(ordered);
        work.beginOutput(merged.length, "text/index");
        writeAll(work, merged);
        work.finishOutput();
        return DurableHost.Result.succeeded();
      } finally {
        deleteTree(scratch);
      }
    };
  }

  static String trim(String text) {
    return text.length() <= 400 ? text : text.substring(0, 400);
  }

  static void deleteTree(Path root) throws Exception {
    if (!Files.exists(root)) return;
    try (var stream = Files.walk(root)) {
      for (Path path : stream.sorted(java.util.Comparator.reverseOrder()).toList())
        Files.deleteIfExists(path);
    }
  }
}
