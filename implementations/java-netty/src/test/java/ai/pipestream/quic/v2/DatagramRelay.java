package ai.pipestream.quic.v2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A seeded UDP relay between a test peer and a listener: one socket faces the client, one faces
 * the server. Each direction can drop a fraction of datagrams and hold one datagram back behind
 * its successor now and then (loss and reordering), and the server-facing socket can be replaced
 * mid-connection so the listener sees the same connection arrive from a new address (a NAT
 * rebinding, the passive form of connection migration). The relay counts what it actually did,
 * so a test cannot pass with the injection idle.
 */
final class DatagramRelay implements AutoCloseable {
  final DatagramChannel front = DatagramChannel.open();
  final AtomicReference<DatagramChannel> back = new AtomicReference<>();
  final InetSocketAddress server;
  final AtomicReference<SocketAddress> client = new AtomicReference<>();
  final AtomicLong forwarded = new AtomicLong();
  final AtomicLong dropped = new AtomicLong();
  final AtomicLong reordered = new AtomicLong();
  final AtomicLong rebinds = new AtomicLong();
  final AtomicLong forwardedSinceRebind = new AtomicLong();
  final int dropPercent;
  final int holdEvery;
  final long seed;
  final Thread toServer;
  volatile Thread toClient;
  volatile boolean closed;

  DatagramRelay(InetSocketAddress server, long seed, int dropPercent, int holdEvery)
      throws Exception {
    this.server = server;
    this.seed = seed;
    this.dropPercent = dropPercent;
    this.holdEvery = holdEvery;
    front.bind(new InetSocketAddress("127.0.0.1", 0));
    front.configureBlocking(false);
    back.set(open());
    toServer = new Thread(() -> pump(front, null, new Random(seed), true), "relay-to-server");
    toServer.setDaemon(true);
    toServer.start();
    toClient = startToClient(back.get());
  }

  private static DatagramChannel open() throws IOException {
    DatagramChannel channel = DatagramChannel.open();
    channel.bind(new InetSocketAddress("127.0.0.1", 0));
    channel.configureBlocking(false);
    return channel;
  }

  private Thread startToClient(DatagramChannel channel) {
    Thread thread =
        new Thread(
            () -> pump(channel, front, new Random(seed ^ 0x5eed ^ rebinds.get()), false),
            "relay-to-client-" + rebinds.get());
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  InetSocketAddress address() throws Exception {
    return (InetSocketAddress) front.getLocalAddress();
  }

  /** The server-facing socket's current address: what the listener sees as the peer. */
  InetSocketAddress serverFacingAddress() throws Exception {
    return (InetSocketAddress) back.get().getLocalAddress();
  }

  /**
   * Replace the server-facing socket: from now on the client's datagrams reach the listener from
   * a new source port, and replies to the old port are lost. The old reader thread ends with its
   * socket.
   *
   * @return the new server-facing address
   */
  InetSocketAddress rebind() throws Exception {
    DatagramChannel replacement = open();
    DatagramChannel old = back.getAndSet(replacement);
    rebinds.incrementAndGet();
    forwardedSinceRebind.set(0);
    old.close();
    toClient.join(5000);
    toClient = startToClient(replacement);
    return (InetSocketAddress) replacement.getLocalAddress();
  }

  private void pump(DatagramChannel in, DatagramChannel fixedOut, Random random, boolean inbound) {
    ByteBuffer buffer = ByteBuffer.allocate(65_536);
    byte[] held = null;
    long count = 0;
    try (Selector selector = Selector.open()) {
      in.register(selector, SelectionKey.OP_READ);
      while (!closed && in.isOpen()) {
        DatagramChannel out = inbound ? back.get() : fixedOut;
        buffer.clear();
        SocketAddress source = in.receive(buffer);
        if (source == null) {
          // Nothing behind a held datagram within the hold window: send it on its own, so a
          // final packet (a credit update after a refusal) is delayed, never withheld.
          if (held != null) {
            SocketAddress target = inbound ? server : client.get();
            if (target != null) {
              out.send(ByteBuffer.wrap(held), target);
              forwarded.incrementAndGet();
            }
            held = null;
          }
          selector.select(20);
          selector.selectedKeys().clear();
          continue;
        }
        if (inbound) client.set(source);
        SocketAddress target = inbound ? server : client.get();
        if (target == null) continue;
        buffer.flip();
        byte[] datagram = new byte[buffer.remaining()];
        buffer.get(datagram);
        count++;
        // Never touch the first packets of the handshake: the clauses under test are about an
        // established connection, and a lost Initial only delays the test.
        if (count > 8 && random.nextInt(100) < dropPercent) {
          dropped.incrementAndGet();
          continue;
        }
        if (held != null) {
          out.send(ByteBuffer.wrap(datagram), target);
          out.send(ByteBuffer.wrap(held), target);
          forwarded.addAndGet(2);
          if (inbound) forwardedSinceRebind.addAndGet(2);
          reordered.incrementAndGet();
          held = null;
          continue;
        }
        if (count > 8 && holdEvery > 0 && count % holdEvery == 0) {
          held = datagram;
          continue;
        }
        out.send(ByteBuffer.wrap(datagram), target);
        forwarded.incrementAndGet();
        if (inbound) forwardedSinceRebind.incrementAndGet();
      }
    } catch (Exception failure) {
      if (!closed && in.isOpen())
        throw new IllegalStateException("relay " + (inbound ? "in" : "out"), failure);
    }
  }

  @Override
  public void close() throws IOException {
    closed = true;
    front.close();
    back.get().close();
    try {
      toServer.join(5000);
      toClient.join(5000);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
