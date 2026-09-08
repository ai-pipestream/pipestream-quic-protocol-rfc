package ai.pipestream.quic.v2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;

/**
 * Operator principal mapping in the same UTF-8 TSV form as the Rust CLI: a {@code
 * sha256\tprincipal} header, then rows of 64 hexadecimal digits of a client leaf DER SHA-256 and a
 * stable owner label. At most 4,096 rows and 1 MiB; duplicates, empty maps and invalid labels are
 * refused.
 */
public final class PrincipalMap {
  private PrincipalMap() {}

  /**
   * Parse a principal map file.
   *
   * @param file regular file, not a symlink
   * @return immutable mapping
   * @throws IOException unreadable, oversized, malformed or ambiguous map
   */
  public static Map<Records.Digest, String> read(Path file) throws IOException {
    if (!Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS))
      throw new IOException("principal map must be a regular file");
    if (Files.size(file) > 1 << 20) throw new IOException("principal map exceeds 1 MiB");
    String text = Files.readString(file, StandardCharsets.UTF_8);
    String[] lines = text.split("\n", -1);
    if (lines.length == 0 || !lines[0].equals("sha256\tprincipal"))
      throw new IOException("principal map header must be sha256<TAB>principal");
    Map<Records.Digest, String> map = new HashMap<>();
    for (int i = 1; i < lines.length; i++) {
      String line = lines[i];
      if (line.isEmpty() && i == lines.length - 1) break;
      String[] columns = line.split("\t", -1);
      if (columns.length != 2 || columns[0].length() != 64)
        throw new IOException("principal map row " + i + " is malformed");
      byte[] digest;
      try {
        digest = HexFormat.of().parseHex(columns[0]);
      } catch (IllegalArgumentException invalid) {
        throw new IOException("principal map row " + i + " has an invalid fingerprint");
      }
      String label;
      try {
        label = Checks.identity(columns[1]);
      } catch (RuntimeException invalid) {
        throw new IOException("principal map row " + i + " has an invalid label");
      }
      if (map.put(new Records.Digest(digest), label) != null)
        throw new IOException("principal map row " + i + " repeats a fingerprint");
      if (map.size() > 4096) throw new IOException("principal map exceeds 4096 rows");
    }
    if (map.isEmpty()) throw new IOException("principal map is empty");
    return Map.copyOf(map);
  }
}
