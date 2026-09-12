package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A raw, scripted V2 authority for client-side negative testing. It negotiates the durable
 * profiles, binds any owner to one fixed generation, answers manifests from a fixed record and
 * delivers each READ through a caller-supplied script that may write anything at all on the result
 * stream. It has no store, no execution and no protocol validation beyond what the script does; it
 * exists so the independent client can be driven with malformed result streams that a conformant
 * authority never produces.
 */
final class RawDurableAuthority implements AutoCloseable {
  /**
   * Writes the result stream for one READ; runs off the event loop, so blocking writes are fine.
   */
  interface ResultScript {
    void deliver(Read read, QuicStreamChannel stream) throws Exception;
  }

  /**
   * Handles one incoming input stream once its header has been read; runs on the event loop.
   * {@code reply} writes a control frame to the same connection from any thread.
   */
  interface InputScript {
    void accept(Records.InputHeader header, QuicStreamChannel stream, Consumer<Message> reply)
        throws Exception;
  }

  /** Answers a control request before the default script; null falls through to the default. */
  interface ControlScript {
    Message answer(Message request);
  }

  private final MultiThreadIoEventLoopGroup group =
      new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
  private final ExecutorService scripts =
      Executors.newCachedThreadPool(
          Thread.ofPlatform().daemon().name("raw-authority-", 0).factory());
  private final DurableOptions options = DurableOptions.defaults();
  private final Records.Manifest manifest;
  private final Records.Policy policy;
  private final Channel listener;
  final BlockingQueue<Message> received = new LinkedBlockingQueue<>();
  /** The client connection's close event, as the transport reported it to this authority. */
  final java.util.concurrent.CompletableFuture<io.netty.handler.codec.quic.QuicConnectionCloseEvent>
      clientClosed = new java.util.concurrent.CompletableFuture<>();
  volatile ResultScript script = (read, stream) -> stream.shutdownOutput().sync();
  /** Default: stop the input at once (STOP_SENDING 0) and never answer it. */
  volatile InputScript inputs = (header, stream, reply) -> stream.close();
  volatile ControlScript controls = request -> null;
  /** Delay before answering any control the raw authority refuses by default (WATCH, ...). */
  volatile long controlDelayMs;
  /** Never answer controls the raw authority refuses by default; the client must bound the wait. */
  volatile boolean withholdControls;

  RawDurableAuthority(
      TlsAuthentication authentication, Records.Manifest manifest, Records.Policy policy)
      throws Exception {
    this.manifest = manifest;
    this.policy = policy;
    StreamTransport.Limits limits = options.transportLimits();
    var codec =
        limits
            .configure(new QuicServerCodecBuilder(), true)
            .version(1)
            .sslEngineProvider(c -> authentication.engine(c.alloc(), 0))
            .maxIdleTimeout(30, TimeUnit.SECONDS)
            .option(QuicChannelOption.STREAM_SEND_BUFFER_LIMITS, limits.nativeSendLimits())
            .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel channel) {
                    TlsAuthentication.Guard guard = authentication.guard();
                    channel.pipeline().addLast(guard, new Control(guard));
                    channel
                        .pipeline()
                        .addLast(
                            new io.netty.channel.ChannelInboundHandlerAdapter() {
                              @Override
                              public void userEventTriggered(
                                  io.netty.channel.ChannelHandlerContext ctx, Object event) {
                                if (event
                                    instanceof io.netty.handler.codec.quic.QuicConnectionCloseEvent
                                        close) clientClosed.complete(close);
                                ctx.fireUserEventTriggered(event);
                              }
                            });
                  }
                })
            .streamHandler(
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel stream) {
                    Control control = stream.parent().pipeline().get(Control.class);
                    if (stream.type() != QuicStreamType.BIDIRECTIONAL) {
                      stream.pipeline().addLast(control.inputReader(stream));
                      return;
                    }
                    stream.config().setAllowHalfClosure(true);
                    control.stream = stream;
                    stream.pipeline().addLast(control.reader());
                  }
                })
            .build();
    listener =
        new Bootstrap()
            .group(group)
            .channel(NioDatagramChannel.class)
            .handler(codec)
            .bind(new InetSocketAddress("127.0.0.1", 0))
            .sync()
            .channel();
  }

  InetSocketAddress address() {
    return (InetSocketAddress) listener.localAddress();
  }

  /** Encode a result header exactly as the reference authority would. */
  static byte[] header(Records.ResultHeader header) {
    return Wire.encodeHeader(header);
  }

  static void write(QuicStreamChannel stream, byte[] bytes) throws InterruptedException {
    stream.writeAndFlush(Unpooled.wrappedBuffer(bytes)).sync();
  }

  static void fin(QuicStreamChannel stream) throws InterruptedException {
    stream.shutdownOutput().sync();
  }

  private final class Control extends io.netty.channel.ChannelInboundHandlerAdapter {
    final TlsAuthentication.Guard guard;
    final Wire.Decoder decoder = new Wire.Decoder(4096);
    QuicStreamChannel stream;
    Capabilities selected;

    Control(TlsAuthentication.Guard guard) {
      this.guard = guard;
    }

    SimpleChannelInboundHandler<ByteBuf> reader() {
      return new SimpleChannelInboundHandler<>() {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) {
          ByteBuffer source = bytes.nioBuffer();
          while (source.hasRemaining()) {
            Wire.Frame frame = decoder.feed(source);
            if (frame == null) break;
            if (frame instanceof Wire.Known known) handle(known.message());
          }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
          ctx.close();
        }
      };
    }

    private void send(Message message) {
      stream.writeAndFlush(Unpooled.wrappedBuffer(Wire.encode(message, selected.controlLimit())));
    }

    /** Read one input header, then hand the stream to the input script. */
    SimpleChannelInboundHandler<ByteBuf> inputReader(QuicStreamChannel input) {
      ObjectStream.HeaderReader reader =
          new ObjectStream.HeaderReader(true, System.nanoTime(), 10_000);
      return new SimpleChannelInboundHandler<>() {
        boolean handed;

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) throws Exception {
          if (handed) return;
          Records.Value header = reader.feed(bytes.nioBuffer(), System.nanoTime());
          if (header == null) return;
          handed = true;
          inputs.accept((Records.InputHeader) header, input, Control.this::send);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
          ctx.close();
        }
      };
    }

    /**
     * A genuine declaration receipt for a scope-0 declaration: the digest the client journals for
     * the normalized request, the accepted count, and the seal over the declared members when
     * sealed, so a client can hold its covering receipt before sending an input.
     */
    private Records.OperationReceipt declared(Declare d) {
      Commitments.Context context =
          new Commitments.Context(manifest.authority(), manifest.owner(), manifest.generation());
      Records.Digest seal = null;
      if (d.seal()) {
        Commitments.Seal sealing =
            new Commitments.Seal(context, d.scope(), 0, null, d.entityIds().size());
        for (long member : d.entityIds()) sealing.add(member);
        seal = sealing.finish();
      }
      return new Records.OperationReceipt(
          d.operation(),
          Commitments.operation(context, 0, ClientJournal.withRequest(d, 1)),
          new Records.Declared(d.scope(), 0, d.entityIds().size(), d.entityIds().size(), seal));
    }

    private void handle(Message message) {
      received.add(message);
      if (selected == null) {
        Capabilities offer = (Capabilities) message;
        selected = guard.negotiate(offer, options.offer());
        decoder.limit(selected.controlLimit());
        send(selected);
        return;
      }
      Message scripted = controls.answer(message);
      if (scripted != null) {
        send(scripted);
        return;
      }
      switch (message) {
        case Create c ->
            send(
                new Binding(
                    c.request(),
                    manifest.authority(),
                    manifest.owner(),
                    manifest.generation(),
                    c.creationSequence(),
                    c.policy(),
                    LIMITS));
        case Attach a ->
            send(
                new Binding(
                    a.request(),
                    manifest.authority(),
                    manifest.owner(),
                    manifest.generation(),
                    1,
                    policy,
                    LIMITS));
        case GetManifest g -> send(new ManifestResponse(g.request(), manifest));
        case Declare d -> send(new DeclarationResponse(d.request(), declared(d)));
        case Read r -> {
          ResultScript current = script;
          stream
              .parent()
              .createStream(
                  QuicStreamType.UNIDIRECTIONAL,
                  new io.netty.channel.ChannelInboundHandlerAdapter())
              .addListener(
                  opened -> {
                    if (!opened.isSuccess()) return;
                    QuicStreamChannel result = (QuicStreamChannel) opened.getNow();
                    scripts.execute(
                        () -> {
                          try {
                            current.deliver(r, result);
                          } catch (Exception failure) {
                            result.close();
                          }
                        });
                  });
        }
        case Detach d -> {
          stream
              .writeAndFlush(
                  Unpooled.wrappedBuffer(
                      Wire.encode(new Detached(d.request()), selected.controlLimit())))
              .addListener(ignored -> stream.shutdownOutput());
        }
        default -> {
          if (withholdControls) return;
          Refusal refusal =
              new Refusal(
                  new Records.RequestTag(false, ClientCorrelation.requestId(message)),
                  ProtocolError.Code.NOT_FOUND,
                  "raw authority");
          if (controlDelayMs > 0)
            stream.eventLoop().schedule(() -> send(refusal), controlDelayMs, TimeUnit.MILLISECONDS);
          else send(refusal);
        }
      }
    }
  }

  private static final Records.Limits LIMITS =
      new Records.Limits(4096, 1_000_000, 1_000_000, 1L << 30, 1L << 30, 16);

  @Override
  public void close() {
    listener.close().syncUninterruptibly();
    scripts.shutdownNow();
    group.shutdownGracefully(0, 1, TimeUnit.SECONDS).syncUninterruptibly();
  }
}
