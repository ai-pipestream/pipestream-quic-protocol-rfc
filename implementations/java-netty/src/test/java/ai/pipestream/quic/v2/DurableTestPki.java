package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Temporary EC mTLS identities shared by the durable endpoint tests. */
final class DurableTestPki {
  final Path directory;

  private DurableTestPki(Path directory) {
    this.directory = directory;
  }

  /**
   * Generate a CA, a server certificate for {@code localhost}/127.0.0.1 and client certificates.
   *
   * @param directory writable directory
   * @param clients client names, each with clientAuth usage
   * @return generated identities
   * @throws Exception openssl failure
   */
  static DurableTestPki generate(Path directory, List<String> clients) throws Exception {
    DurableTestPki pki = new DurableTestPki(directory);
    pki.command(
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-noenc",
        "-keyout",
        "ca.key",
        "-out",
        "ca.crt",
        "-days",
        "2",
        "-subj",
        "/CN=Durable-Test-CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign");
    pki.leaf("server", "serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n");
    for (String client : clients) pki.leaf(client, "clientAuth\n");
    return pki;
  }

  private void leaf(String name, String usage) throws Exception {
    command(
        "openssl",
        "req",
        "-new",
        "-newkey",
        "ec",
        "-pkeyopt",
        "ec_paramgen_curve:prime256v1",
        "-noenc",
        "-keyout",
        name + ".key",
        "-out",
        name + ".csr",
        "-subj",
        "/CN=" + name);
    Files.writeString(
        path(name + ".ext"),
        "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage="
            + usage);
    command(
        "openssl",
        "x509",
        "-req",
        "-in",
        name + ".csr",
        "-CA",
        "ca.crt",
        "-CAkey",
        "ca.key",
        "-CAcreateserial",
        "-out",
        name + ".crt",
        "-days",
        "2",
        "-extfile",
        name + ".ext");
  }

  Path path(String name) {
    return directory.resolve(name);
  }

  X509Certificate certificate(String name) throws Exception {
    try (var input = Files.newInputStream(path(name + ".crt"))) {
      return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(input);
    }
  }

  Records.Digest fingerprint(String name) throws Exception {
    return TlsAuthentication.fingerprint(certificate(name));
  }

  String hexFingerprint(String name) throws Exception {
    return java.util.HexFormat.of()
        .formatHex(MessageDigest.getInstance("SHA-256").digest(certificate(name).getEncoded()));
  }

  /**
   * Principal mapping for named clients, each mapped to its own name.
   *
   * @param clients mapped client names
   * @return mutable mapping
   * @throws Exception certificate failure
   */
  Map<Records.Digest, String> principals(List<String> clients) throws Exception {
    Map<Records.Digest, String> map = new HashMap<>();
    for (String client : clients) map.put(fingerprint(client), client);
    return map;
  }

  TlsAuthentication server(Map<Records.Digest, String> principals) throws Exception {
    return TlsAuthentication.server(
        path("ca.crt"), path("server.crt"), path("server.key"), principals, Clock.systemUTC());
  }

  TlsAuthentication client(String name) throws Exception {
    return TlsAuthentication.client(
        path("ca.crt"),
        "localhost",
        name == null ? null : path(name + ".crt"),
        name == null ? null : path(name + ".key"));
  }

  /**
   * Write a Rust-compatible principal map TSV.
   *
   * @param file destination
   * @param clients mapped client names
   * @throws Exception I/O or certificate failure
   */
  void principalMap(Path file, List<String> clients) throws Exception {
    StringBuilder text = new StringBuilder("sha256\tprincipal\n");
    for (String client : clients)
      text.append(hexFingerprint(client)).append('\t').append(client).append('\n');
    Files.writeString(file, text.toString());
  }

  private void command(String... args) throws Exception {
    Path log = Files.createTempFile(directory, "openssl-", ".log");
    Process process =
        new ProcessBuilder(args)
            .directory(directory.toFile())
            .redirectErrorStream(true)
            .redirectOutput(log.toFile())
            .start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      assertEquals(0, process.exitValue(), () -> log.toString());
    } finally {
      if (process.isAlive()) process.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
    }
  }
}
