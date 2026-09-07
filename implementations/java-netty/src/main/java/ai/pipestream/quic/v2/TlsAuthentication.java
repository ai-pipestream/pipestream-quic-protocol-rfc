package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.ProtocolError.Code.*;

import ai.pipestream.quic.TlsPeerIdentity;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicSslEngine;
import io.netty.util.ReferenceCountUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * QUIC/TLS authentication boundary for the independent V2 endpoint.
 *
 * <p>Install a fresh {@link Guard} first in each connection pipeline and require it from every
 * stream dispatcher before application access. It delays downstream channel activation until
 * authentication succeeds. This is not an endpoint, durable authorization store or profile
 * implementation. Applications still check session policy/revocation inside committing
 * transactions.
 *
 * <p>Both initial and resumed handshakes revalidate the peer chain before application activation.
 * Clients do not cache sessions; servers also handle external clients that do resume. No
 * application 0-RTT is enabled. Certificate failures use TLS transport errors, never application
 * refusals.
 */
public final class TlsAuthentication {
  /** Version-2 application protocol, never negotiated as version 1. */
  public static final String ALPN = "pipestream/2";

  private final boolean server;
  private final String reference;
  private final X509TrustManager trust;
  private final Clock clock;
  private final QuicSslContext context;
  private volatile Map<Records.Digest, String> principals;

  private TlsAuthentication(
      boolean server,
      Path roots,
      Path certificate,
      Path key,
      String reference,
      Map<Records.Digest, String> principals,
      Clock clock)
      throws IOException, GeneralSecurityException {
    this.server = server;
    this.reference = reference;
    this.clock = Objects.requireNonNull(clock);
    this.principals = checkedPrincipals(principals);
    trust = trust(roots);
    var builder =
        server
            ? QuicSslContextBuilder.forServer(key.toFile(), null, certificate.toFile())
                .clientAuth(ClientAuth.OPTIONAL)
            : QuicSslContextBuilder.forClient().sessionCacheSize(0);
    if (!server && certificate != null)
      builder.keyManager(key.toFile(), null, certificate.toFile());
    context =
        builder.trustManager(new Verifier()).applicationProtocols(ALPN).earlyData(false).build();
  }

  /**
   * Configure a server which requests and validates optional caller certificates. Missing identity
   * is permitted only for Core; required durable negotiation must fail before its response.
   *
   * @param roots explicitly trusted client CA certificates, bounded PEM file
   * @param certificate server certificate chain PEM
   * @param key server unencrypted PKCS#8 private key PEM
   * @param principals complete verified DER-leaf SHA-256 to stable owner mapping
   * @param clock current credential-validity clock; normally {@link Clock#systemUTC()}
   * @return reusable server authentication configuration
   * @throws IOException for configuration I/O failure
   * @throws GeneralSecurityException for invalid trust configuration
   */
  public static TlsAuthentication server(
      Path roots, Path certificate, Path key, Map<Records.Digest, String> principals, Clock clock)
      throws IOException, GeneralSecurityException {
    return new TlsAuthentication(
        true,
        roots,
        Objects.requireNonNull(certificate),
        Objects.requireNonNull(key),
        null,
        principals,
        clock);
  }

  /**
   * Configure a client with an independent service reference, and optional caller credentials.
   *
   * @param roots explicitly trusted server CA certificates, bounded PEM file
   * @param reference trusted DNS name (ASCII A-labels) or literal IP, not a locator's claim
   * @param certificate caller chain PEM, or null for Core-only anonymous authentication
   * @param key unencrypted PKCS#8 caller key PEM, null exactly when certificate is null
   * @return reusable client authentication configuration
   * @throws IOException for configuration I/O failure
   * @throws GeneralSecurityException for invalid trust configuration
   */
  public static TlsAuthentication client(Path roots, String reference, Path certificate, Path key)
      throws IOException, GeneralSecurityException {
    if (reference == null
        || reference.isEmpty()
        || reference.length() > 253
        || (certificate == null) != (key == null))
      throw new IllegalArgumentException("invalid service reference or incomplete credential");
    return new TlsAuthentication(
        false, roots, certificate, key, reference, Map.of(), Clock.systemUTC());
  }

  /**
   * Construct an engine for a QUIC codec's engine provider.
   *
   * @param allocator connection allocator
   * @param port independently configured remote port (ignored by servers)
   * @return configured QUIC engine; native transport must perform the handshake
   */
  public QuicSslEngine engine(ByteBufAllocator allocator, int port) {
    return server ? context.newEngine(allocator) : context.newEngine(allocator, reference, port);
  }

  /**
   * Create one non-sharable authentication guard per QUIC connection.
   *
   * @return new guard, to be installed before application handlers
   */
  public Guard guard() {
    return new Guard();
  }

  /**
   * Atomically replace this server's bounded principal mapping. Existing connections cannot change
   * owners: removed or remapped credentials lose request authorization. Rotations may map multiple
   * certificates to the same owner. This does not revoke an already accepted job's separate grant.
   *
   * @param replacement complete new configured mapping
   */
  public void replacePrincipals(Map<Records.Digest, String> replacement) {
    if (!server) throw new IllegalStateException("client has no principal map");
    principals = checkedPrincipals(replacement);
  }

  /**
   * Compute a mapping key, not an authentication assertion.
   *
   * @param certificate full leaf certificate
   * @return SHA-256 of its complete DER encoding
   * @throws CertificateException if the certificate cannot be encoded
   */
  public static Records.Digest fingerprint(X509Certificate certificate)
      throws CertificateException {
    try {
      return new Records.Digest(
          MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded()));
    } catch (java.security.NoSuchAlgorithmException impossible) {
      throw new AssertionError(impossible);
    }
  }

  private static Map<Records.Digest, String> checkedPrincipals(Map<Records.Digest, String> values) {
    Objects.requireNonNull(values);
    if (values.size() > 16384)
      throw new IllegalArgumentException("too many configured credentials");
    var copy = new HashMap<Records.Digest, String>();
    values.forEach(
        (digest, owner) -> copy.put(Objects.requireNonNull(digest), Checks.identity(owner)));
    return Map.copyOf(copy);
  }

  private static X509TrustManager trust(Path roots) throws IOException, GeneralSecurityException {
    byte[] pem;
    try (var input = Files.newInputStream(roots)) {
      pem = input.readNBytes(1048577);
    }
    if (pem.length == 0 || pem.length > 1048576)
      throw new CertificateException("invalid trust-file size");
    var certificates =
        CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(pem));
    if (certificates.isEmpty() || certificates.size() > 256)
      throw new CertificateException("invalid trust-anchor count");
    var store = KeyStore.getInstance(KeyStore.getDefaultType());
    store.load(null, null);
    int index = 0;
    for (var certificate : certificates)
      store.setCertificateEntry(Integer.toString(index++), certificate);
    var factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    factory.init(store);
    for (var manager : factory.getTrustManagers())
      if (manager instanceof X509TrustManager x509) return x509;
    throw new GeneralSecurityException("missing X509 trust manager");
  }

  private void verify(X509Certificate[] chain, String algorithm, boolean client)
      throws CertificateException {
    if (chain == null || chain.length == 0 || chain.length > 16)
      throw new CertificateException("invalid peer chain count");
    long total = 0;
    Instant now = clock.instant();
    for (var certificate : chain) {
      total += certificate.getEncoded().length;
      if (total > 65536) throw new CertificateException("peer chain exceeds verification bound");
      certificate.checkValidity(java.util.Date.from(now));
      if (!now.isBefore(certificate.getNotAfter().toInstant()))
        throw new CertificateExpiredException("credential expired");
    }
    if (client) trust.checkClientTrusted(chain, algorithm);
    else {
      trust.checkServerTrusted(chain, algorithm);
      try {
        TlsPeerIdentity.verify(chain[0], reference);
      } catch (SSLPeerUnverifiedException mismatch) {
        throw new CertificateException("service identity mismatch", mismatch);
      }
    }
  }

  private final class Verifier extends X509ExtendedTrustManager {
    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return trust.getAcceptedIssuers();
    }

    @Override
    public void checkClientTrusted(X509Certificate[] c, String a) throws CertificateException {
      verify(c, a, true);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] c, String a) throws CertificateException {
      verify(c, a, false);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] c, String a, SSLEngine e)
        throws CertificateException {
      verify(c, a, true);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] c, String a, SSLEngine e)
        throws CertificateException {
      verify(c, a, false);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] c, String a, Socket s)
        throws CertificateException {
      verify(c, a, true);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] c, String a, Socket s)
        throws CertificateException {
      verify(c, a, false);
    }
  }

  /**
   * Connection-confined guard. Stream owners must call {@link #requireAuthenticated()} before Core
   * access and {@link #requireOwner()} before durable access, including again at commit. Successful
   * handshake alone does not activate any profile or authorize any session.
   */
  public final class Guard extends ChannelInboundHandlerAdapter {
    private final CompletableFuture<Void> readiness = new CompletableFuture<>();
    private boolean activePending;
    private volatile boolean authenticated;
    private volatile boolean ended;
    private Records.Digest fingerprint;
    private String owner;
    private Instant validFrom;
    private Instant validUntil;

    private Guard() {}

    /**
     * Observe authentication completion without being able to complete the underlying promise.
     *
     * @return read-only completion stage; connection loss before readiness fails it
     */
    public CompletionStage<Void> ready() {
      return readiness.minimalCompletionStage();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      if (authenticated) ctx.fireChannelActive();
      else activePending = true;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
      if (event instanceof SslHandshakeCompletionEvent handshake) {
        if (ended || authenticated) return;
        if (!handshake.isSuccess()) {
          ended = true;
          readiness.completeExceptionally(handshake.cause());
          // The QUIC engine owns its TLS alert; do not replace it with an application close.
          ctx.fireUserEventTriggered(event);
          return;
        }
        try {
          var engine = ((QuicChannel) ctx.channel()).sslEngine();
          if (engine == null
              || !ALPN.equals(engine.getApplicationProtocol())
              || !"TLSv1.3".equals(engine.getSession().getProtocol()))
            throw new SSLHandshakeException("unexpected TLS version or ALPN");
          java.security.cert.Certificate[] certificates;
          try {
            certificates = engine.getSession().getPeerCertificates();
          } catch (SSLPeerUnverifiedException absent) {
            if (!server) throw absent;
            certificates = new java.security.cert.Certificate[0];
          }
          if (certificates.length != 0) {
            if (certificates.length > 16) throw new CertificateException("peer chain count");
            X509Certificate[] chain = new X509Certificate[certificates.length];
            for (int n = 0; n < chain.length; n++) {
              if (!(certificates[n] instanceof X509Certificate x509))
                throw new CertificateException("non-X509 peer");
              chain[n] = x509;
            }
            // Resumption can bypass the native trust callback. Revalidate retained chain and name.
            verify(chain, server ? chain[0].getPublicKey().getAlgorithm() : "UNKNOWN", server);
            fingerprint = fingerprint(chain[0]);
            owner = server ? principals.get(fingerprint) : null;
            for (var certificate : chain) {
              Instant start = certificate.getNotBefore().toInstant(),
                  end = certificate.getNotAfter().toInstant();
              if (validFrom == null || start.isAfter(validFrom)) validFrom = start;
              if (validUntil == null || end.isBefore(validUntil)) validUntil = end;
            }
          }
          authenticated = true;
          if (activePending) {
            activePending = false;
            ctx.fireChannelActive();
          }
          readiness.complete(null);
          ctx.fireUserEventTriggered(event);
        } catch (GeneralSecurityException | IOException failure) {
          ended = true;
          readiness.completeExceptionally(failure);
          int alert = failure instanceof CertificateExpiredException ? 45 : 42;
          ((QuicChannel) ctx.channel()).close(false, 0x100 + alert, Unpooled.EMPTY_BUFFER);
        }
      } else ctx.fireUserEventTriggered(event);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
      if (!authenticated || ended) ReferenceCountUtil.release(message);
      else ctx.fireChannelRead(message);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      ended = true;
      readiness.completeExceptionally(new IOException("connection ended before authentication"));
      ctx.fireChannelInactive();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      ended = true;
      readiness.completeExceptionally(
          new IOException("connection unregistered before authentication"));
      ctx.fireChannelUnregistered();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      ended = true;
      readiness.completeExceptionally(cause);
      ctx.fireExceptionCaught(cause);
      // Reentrant close here can free the native connection before it flushes its TLS alert.
      if (!(cause instanceof javax.net.ssl.SSLException)) ctx.close();
    }

    /**
     * Check current credential validity before application access. No durable outcome is inferred.
     */
    public void requireAuthenticated() {
      if (!authenticated || ended) throw denied();
      Instant now = clock.instant();
      if (validUntil != null && (!now.isBefore(validUntil) || now.isBefore(validFrom)))
        throw denied();
    }

    /**
     * Recheck credential validity and this connection's original owner against current mapping.
     *
     * @return exact configured stable owner, never supplied by the peer
     */
    public String requireOwner() {
      requireAuthenticated();
      if (!server || owner == null || !owner.equals(principals.get(fingerprint))) throw denied();
      return owner;
    }

    /**
     * Apply caller authentication to a capability exchange. This helper does not send its result;
     * the endpoint must close on a required-identity denial before sending any capabilities
     * response. Only fully implemented profiles may appear in the local offer.
     *
     * @param offer decoded client offer
     * @param local server's actually implemented offer
     * @return selected capabilities, with unauthorized optional profiles excluded
     */
    public Capabilities negotiate(Capabilities offer, Capabilities local) {
      if (!server) throw new IllegalStateException("client does not select profiles");
      requireAuthenticated();
      ProtocolError.require(!offer.response() && !local.response(), "negotiation requires offers");
      boolean caller = owner != null && owner.equals(principals.get(fingerprint));
      if (!caller) {
        if (offer.required().stream().anyMatch(TlsAuthentication::requiresCaller)
            || local.required().stream().anyMatch(TlsAuthentication::requiresCaller))
          throw denied();
        local =
            new Capabilities(
                false,
                local.supported().stream()
                    .filter(p -> p != DURABLE_WORK && p != RESULT_DELIVERY)
                    .toList(),
                local.required(),
                local.controlLimit(),
                local.streamLimit(),
                local.pendingLimit(),
                local.objectLimit(),
                local.streamIdleMs(),
                local.streamLifetimeMs());
      }
      return Capabilities.negotiate(offer, local);
    }
  }

  private static ProtocolError denied() {
    return new ProtocolError(UNAUTHORIZED, "caller credential unavailable");
  }

  private static boolean requiresCaller(int profile) {
    return profile == DURABLE_WORK || profile == RESULT_DELIVERY;
  }
}
