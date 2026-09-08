package ai.pipestream.quic.v2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.handler.codec.quic.Quic;
import io.netty.handler.codec.quic.QuicStreamSendBufferLimits;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;

final class TransportDependencyTest {
  private static final String CLASSES_ARTIFACT =
      "netty-codec-classes-quic-4.2.17.Final-pipestream.1.jar";
  private static final String NATIVE_ARTIFACT =
      "netty-codec-native-quic-4.2.17.Final-pipestream.1-linux-x86_64.jar";
  private static final String CUSTOM_NATIVE =
      "META-INF/native/libnetty_quiche42_pipestream_linux_x86_64.so";
  private static final String OFFICIAL_NATIVE = "META-INF/native/libnetty_quiche42_linux_x86_64.so";

  @Test
  void runtimeUsesOnlySourcePinnedTransportExtension() throws Exception {
    Quic.ensureAvailability();

    ClassLoader loader = Quic.class.getClassLoader();
    List<URL> quicClasses = resources(loader, "io/netty/handler/codec/quic/Quic.class");
    assertEquals(1, quicClasses.size());
    URL codeSource = Quic.class.getProtectionDomain().getCodeSource().getLocation();
    assertTrue(
        codeSource.getPath().endsWith("/" + CLASSES_ARTIFACT),
        () -> "unexpected QUIC classes artifact: " + codeSource);
    assertTrue(
        quicClasses.getFirst().toExternalForm().contains(CLASSES_ARTIFACT + "!/"),
        () -> "QUIC class resource is not from expected artifact: " + quicClasses.getFirst());
    assertEquals(
        codeSource,
        QuicStreamSendBufferLimits.class.getProtectionDomain().getCodeSource().getLocation());

    // This linkage is intentionally absent from the official Netty QUIC artifact.
    assertEquals(1, new QuicStreamSendBufferLimits(1, 0).total());

    List<URL> customNative = resources(loader, CUSTOM_NATIVE);
    assertEquals(1, customNative.size(), "custom native library must occur exactly once");
    assertEquals(
        0, resources(loader, OFFICIAL_NATIVE).size(), "official native library is forbidden");

    URL nativeUrl = customNative.getFirst();
    assertEquals(
        "jar",
        nativeUrl.getProtocol(),
        () -> "native resource is not packaged in a JAR: " + nativeUrl);
    JarURLConnection connection = (JarURLConnection) nativeUrl.openConnection();
    connection.setUseCaches(false);
    assertTrue(
        connection.getJarFileURL().getPath().endsWith("/" + NATIVE_ARTIFACT),
        () -> "unexpected QUIC native artifact: " + connection.getJarFileURL());
    assertEquals(CUSTOM_NATIVE, connection.getEntryName());
    try (var jar = connection.getJarFile()) {
      Manifest manifest = jar.getManifest();
      Attributes attributes = manifest.getMainAttributes();
      assertEquals(
          "4f347477006bf7f928335d28f05056013f70b87e", attributes.getValue("Quiche-Revision"));
      assertEquals(
          "0226f30467f540a3f62ef48d453f93927da199b6", attributes.getValue("BoringSSL-Revision"));
      assertEquals(
          "209c4752825bde71527d27fdaaa12f54a7a13c8a6477225d7d5e2d2fefab7881",
          attributes.getValue("PipeStream-Quiche-Patch-SHA256"));
      assertEquals(
          "a6f0cbd615abce2d572763b8d14299f92e9af45fb77e71e55676794d29a771c7",
          attributes.getValue("PipeStream-Netty-Patch-SHA256"));
    }
  }

  private static List<URL> resources(ClassLoader loader, String name) throws Exception {
    return Collections.list(loader.getResources(name));
  }
}
