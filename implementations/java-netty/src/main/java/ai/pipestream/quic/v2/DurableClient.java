package ai.pipestream.quic.v2;

import static ai.pipestream.quic.v2.Messages.*;
import static ai.pipestream.quic.v2.ProtocolError.Code.*;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.channel.FixedRecvByteBufAllocator;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicChannelOption;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicConnectionCloseEvent;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamFrame;
import io.netty.handler.codec.quic.QuicStreamType;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * The independent Java durable client: one authenticated connection, one {@link ClientJournal},
 * typed operations for the whole selected profile combination. Every mutation is journaled before
 * it is sent; every response is validated against journaled identity, recomputed digests and known
 * commitments, then journaled, before the returned stage completes. Connection loss leaves
 * operations unresolved in the journal, never inventing success or new work. Networking runs on an
 * owned event loop; journal and file work runs on an owned single worker thread.
 */
public final class DurableClient implements AutoCloseable {
  /**
   * One ordered membership page after validation.
   *
   * @param scope scope identity
   * @param producer scope producer
   * @param parent parent work, null for root
   * @param declared declared count in this snapshot
   * @param sealed whether the committed seal is present
   * @param seal committed seal or null
   * @param entries members in increasing order with their states
   * @param more whether more members exist beyond this page
   * @param membershipVerified whether the journal now holds the complete verified sealed membership
   */
  public record ScopePage(
      long scope,
      int producer,
      Records.WorkKey parent,
      long declared,
      boolean sealed,
      Records.Digest seal,
      List<Entry> entries,
      boolean more,
      boolean membershipVerified) {}

  private static final class Continuation {
    final Consumer<Message> response;
    final CompletableFuture<?> result;

    Continuation(Consumer<Message> response, CompletableFuture<?> result) {
      this.response = response;
      this.result = result;
    }
  }

  private final TlsAuthentication authentication;
  private final TlsAuthentication.Guard guard;
  private final ClientJournal journal;
  private final ClientOptions options;
  private final CoreOptions core;
  private final StreamTransport.Limits transportLimits;
  private final MultiThreadIoEventLoopGroup group;
  private final EventLoop loop;
  private final ExecutorService worker;
  private final ClientCorrelation correlation;
  private final Capabilities offer;
  private final CompletableFuture<Capabilities> readiness = new CompletableFuture<>();
  private final CompletableFuture<Void> detached = new CompletableFuture<>();
  private final CompletableFuture<Void> termination = new CompletableFuture<>();
  private final Map<Long, Continuation> controls = new HashMap<>();
  private final Map<Long, InputTransfer> inputTransfers = new HashMap<>();
  private final Map<Long, ResultTransfer> resultTransfers = new HashMap<>();
  private final Set<ResultTransfer> incomingResults = new HashSet<>();
  private final AtomicBoolean closeRequested = new AtomicBoolean();
  private final long started = System.nanoTime();

  /** Trusted local durability hooks; {@link Boundaries#NONE} in shipped launchers. */
  private volatile Boundaries boundaries = Boundaries.NONE;

  private CompletableFuture<Binding> bindingStage;
  private long lastFrame = started;
  private long nextRequest = 1;
  private long detachStart;
  private Channel socket;
  private QuicChannel connection;
  private StreamTransport transport;
  private Control control;
  private Capabilities selected;
  private Binding binding;
  private boolean authenticated;
  private boolean detachSent;
  private boolean detachAcknowledged;
  private boolean stopping;
  private Throwable terminalFailure;
  private io.netty.util.concurrent.ScheduledFuture<?> timer;

  private DurableClient(
      TlsAuthentication authentication, ClientJournal journal, ClientOptions options) {
    this.authentication = Objects.requireNonNull(authentication);
    this.journal = Objects.requireNonNull(journal);
    this.options = Objects.requireNonNull(options);
    if (authentication.isServer())
      throw new IllegalArgumentException("client TLS configuration required");
    core = options.core();
    transportLimits = options.transportLimits();
    guard = authentication.guard();
    offer = options.offer(journal.intent().profiles());
    correlation = new ClientCorrelation(offer);
    group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    loop = group.next();
    worker =
        Executors.newSingleThreadExecutor(
            Thread.ofPlatform().daemon().name("pipestream-v2-client-journal").factory());
  }

  /**
   * Bind a local socket and start authenticated negotiation. The offer requires exactly the
   * journaled profile combination; a different selection fails readiness.
   *
   * @param remote resolved remote address with a nonzero port
   * @param authentication client trust, service identity and caller credentials
   * @param journal open exclusive journal
   * @param options bounded policy
   * @return owned client; await {@link #ready()} before operations
   * @throws InterruptedException if binding is interrupted
   */
  public static DurableClient connect(
      InetSocketAddress remote,
      TlsAuthentication authentication,
      ClientJournal journal,
      ClientOptions options)
      throws InterruptedException {
    return connect(remote, authentication, journal, options, Boundaries.NONE);
  }

  /**
   * Connect with test-only boundary hooks. Hooks observe or hold reached boundaries on the journal
   * worker and the event loop; they cannot forge receipts, journal entries or results. Shipped
   * launchers never use this overload.
   *
   * @param remote resolved remote address with a nonzero port
   * @param authentication client trust, service identity and caller credentials
   * @param journal open exclusive journal
   * @param options bounded policy
   * @param hooks boundary hooks
   * @return owned client; await {@link #ready()} before operations
   * @throws InterruptedException if binding is interrupted
   */
  static DurableClient connect(
      InetSocketAddress remote,
      TlsAuthentication authentication,
      ClientJournal journal,
      ClientOptions options,
      Boundaries hooks)
      throws InterruptedException {
    Objects.requireNonNull(remote);
    Objects.requireNonNull(hooks);
    if (remote.isUnresolved() || remote.getPort() == 0)
      throw new IllegalArgumentException("resolved remote with nonzero port required");
    DurableClient client = new DurableClient(authentication, journal, options);
    client.boundaries = hooks;
    boolean success = false;
    try {
      var codec =
          client
              .transportLimits
              .configure(new QuicClientCodecBuilder(), false)
              .version(1)
              .sslEngineProvider(c -> authentication.engine(c.alloc(), remote.getPort()))
              .maxIdleTimeout(
                  Math.max(
                      client.core.handshakeTimeoutMs(),
                      Math.max(client.core.controlTimeoutMs(), client.core.streamLifetimeMs())),
                  TimeUnit.MILLISECONDS)
              .build();
      var binding =
          new Bootstrap()
              .group(client.group)
              .channel(NioDatagramChannel.class)
              .handler(codec)
              .bind(
                  new InetSocketAddress(
                      remote.getAddress().isLoopbackAddress() ? remote.getAddress() : null, 0));
      client.socket = binding.channel();
      binding.sync();
      client.loop.execute(() -> client.start(remote));
      success = true;
      return client;
    } finally {
      if (!success) client.close();
    }
  }

  /**
   * Observe validated capability selection.
   *
   * @return read-only stage
   */
  public CompletionStage<Capabilities> ready() {
    return readiness.minimalCompletionStage();
  }

  /**
   * Observe owned socket/event-loop termination.
   *
   * @return successful after graceful detach, exceptional otherwise
   */
  public CompletionStage<Void> closed() {
    return termination.minimalCompletionStage();
  }

  /**
   * The journal this client owns.
   *
   * @return journal
   */
  public ClientJournal journal() {
    return journal;
  }

  private void start(InetSocketAddress remote) {
    if (stopping) return;
    long interval =
        Math.max(
            1, Math.min(100, Math.min(core.handshakeTimeoutMs(), core.controlTimeoutMs()) / 4));
    timer = loop.scheduleAtFixedRate(this::check, interval, interval, TimeUnit.MILLISECONDS);
    try {
      QuicChannel.newBootstrap(socket)
          .option(QuicChannelOption.STREAM_SEND_BUFFER_LIMITS, transportLimits.nativeSendLimits())
          .handler(
              new ChannelInitializer<QuicChannel>() {
                @Override
                protected void initChannel(QuicChannel channel) {
                  connection = channel;
                  transport = new StreamTransport(channel, transportLimits, false);
                  channel.pipeline().addLast(guard, new Connection());
                  channel
                      .closeFuture()
                      .addListener(
                          ignored -> {
                            if (!stopping)
                              fail(
                                  new ProtocolError(
                                      CONTROL_RESET, "connection ended before drain"));
                          });
                }
              })
          .streamHandler(
              new ChannelInitializer<QuicStreamChannel>() {
                @Override
                protected void initChannel(QuicStreamChannel stream) {
                  incomingStream(stream);
                }
              })
          .remoteAddress(remote)
          .connect()
          .addListener(
              result -> {
                if (!result.isSuccess()) deferFailure(result.cause());
              });
      guard
          .ready()
          .whenComplete(
              (ignored, failure) -> {
                if (failure != null) deferFailure(failure);
              });
    } catch (RuntimeException failure) {
      fail(failure);
    }
  }

  private void deferFailure(Throwable failure) {
    if (!stopping) loop.execute(() -> fail(failure));
  }

  private void check() {
    if (stopping) return;
    try {
      long now = System.nanoTime();
      if (!authenticated) ObjectStream.before(now, started, core.handshakeTimeoutMs() * 1000000L);
      else {
        guard.requireAuthenticated();
        ObjectStream.before(now, lastFrame, core.controlTimeoutMs() * 1000000L);
        if (detachSent)
          ObjectStream.before(now, detachStart, selected.streamLifetimeMs() * 1000000L);
        if (control != null) control.writes.check(now);
        for (ResultTransfer transfer : Set.copyOf(incomingResults)) transfer.check(now);
      }
    } catch (ProtocolError failure) {
      fail(failure);
    }
  }

  private void fail(Throwable failure) {
    if (!stopping) stop(Objects.requireNonNull(failure));
  }

  private void stop(Throwable failure) {
    if (stopping) return;
    stopping = true;
    terminalFailure = failure;
    if (timer != null) timer.cancel(false);
    if (control != null) control.writes.end();
    correlation.close();
    Throwable cause = failure == null ? new ProtocolError(CANCELLED, "client stopped") : failure;
    for (Continuation continuation : List.copyOf(controls.values()))
      continuation.result.completeExceptionally(named(cause));
    controls.clear();
    for (InputTransfer transfer : List.copyOf(inputTransfers.values())) transfer.lost(cause);
    inputTransfers.clear();
    for (ResultTransfer transfer : List.copyOf(resultTransfers.values())) transfer.lost(cause);
    resultTransfers.clear();
    if (failure != null) {
      readiness.completeExceptionally(failure);
      detached.completeExceptionally(failure);
      if (bindingStage != null) bindingStage.completeExceptionally(failure);
    }
    if (connection != null) {
      int code =
          failure instanceof ProtocolError protocol
              ? (int) protocol.code().applicationError()
              : (int) CANCELLED.applicationError();
      connection.close(
          authenticated, failure == null ? 0 : authenticated ? code : 0x0c, Unpooled.EMPTY_BUFFER);
    }
    if (socket != null) socket.close();
    worker.shutdown();
    group
        .shutdownGracefully(0, 2, TimeUnit.SECONDS)
        .addListener(
            ignored -> {
              if (terminalFailure == null) termination.complete(null);
              else termination.completeExceptionally(terminalFailure);
            });
    if (failure == null) detached.complete(null);
  }

  /**
   * Abort an unfinished connection and release owned resources. Outstanding operations stay
   * unresolved in the journal.
   */
  @Override
  public void close() {
    if (closeRequested.compareAndSet(false, true)) {
      if (loop.inEventLoop()) fail(new ProtocolError(CANCELLED, "client closed by caller"));
      else {
        try {
          loop.execute(() -> fail(new ProtocolError(CANCELLED, "client closed by caller")));
        } catch (RuntimeException stopped) {
          // Shutdown already in progress.
        }
      }
    }
    if (!loop.inEventLoop()) group.terminationFuture().awaitUninterruptibly();
  }

  // ---------------------------------------------------------------- request plumbing

  @FunctionalInterface
  private interface Blocking<T> {
    T run() throws Exception;
  }

  private static ProtocolError named(Throwable failure) {
    Throwable cause = failure;
    while (cause != null) {
      if (cause instanceof ProtocolError error) return error;
      cause = cause.getCause();
    }
    if (failure instanceof IOException || failure instanceof SQLException)
      return new ProtocolError(INTERNAL_ERROR, "client storage failed: " + failure.getMessage());
    return new ProtocolError(INTERNAL_ERROR, "client operation failed: " + failure);
  }

  /** Run blocking journal/file work on the worker, then continue on the event loop. */
  private <T> void blocking(Blocking<T> work, Consumer<T> success, Consumer<Throwable> failure) {
    try {
      worker.execute(
          () -> {
            T value;
            try {
              value = work.run();
            } catch (Throwable thrown) {
              onLoop(() -> failure.accept(thrown));
              return;
            }
            onLoop(() -> success.accept(value));
          });
    } catch (RuntimeException rejected) {
      failure.accept(new ProtocolError(CANCELLED, "client stopped"));
    }
  }

  private void onLoop(Runnable action) {
    try {
      loop.execute(action);
    } catch (RuntimeException stopped) {
      // Terminated; the caller's future was already failed by stop().
    }
  }

  private <T> CompletableFuture<T> operation(Consumer<CompletableFuture<T>> body) {
    CompletableFuture<T> result = new CompletableFuture<>();
    try {
      loop.execute(
          () -> {
            if (stopping) {
              result.completeExceptionally(new ProtocolError(CANCELLED, "client stopped"));
              return;
            }
            try {
              body.accept(result);
            } catch (Throwable failure) {
              result.completeExceptionally(failure);
            }
          });
    } catch (RuntimeException stopped) {
      result.completeExceptionally(new ProtocolError(CANCELLED, "client stopped"));
    }
    return result;
  }

  private void requireReady() {
    if (selected == null) throw new ProtocolError(NOT_READY, "negotiation incomplete");
    if (detachSent) throw new ProtocolError(NOT_READY, "connection detaching");
  }

  private void requireBound() {
    requireReady();
    if (binding == null) throw new ProtocolError(NOT_READY, "session not bound; call binding()");
  }

  private Commitments.Context context() {
    return new Commitments.Context(binding.authority(), binding.owner(), binding.generation());
  }

  /** Send one control and route its correlated response, on the event loop. */
  private void send(
      Message request,
      ClientCorrelation.ResultCommitment commitment,
      CompletableFuture<?> result,
      Consumer<Message> response) {
    send(request, commitment, result, response, Boundaries.Details.NONE);
  }

  private void send(
      Message request,
      ClientCorrelation.ResultCommitment commitment,
      CompletableFuture<?> result,
      Consumer<Message> response,
      Boundaries.Details details) {
    long id = ClientCorrelation.requestId(request);
    byte[] frame = correlation.register(request, commitment);
    controls.put(id, new Continuation(response, result));
    if (!control.writes.sendEncoded(
        frame,
        success -> {
          if (success) boundaries.sent(Boundaries.Boundary.REQUEST_SENT, details);
        })) {
      controls.remove(id);
      throw new ProtocolError(CONTROL_RESET, "control write refused");
    }
  }

  private long allocate() {
    return nextRequest++;
  }

  private static ProtocolError refused(Refusal refusal) {
    return ProtocolError.refused(refusal.code(), refusal.detail());
  }

  // ---------------------------------------------------------------- session

  /**
   * Replay the journaled creation, or attach to the journaled binding, then validate and journal
   * the response. Repeated calls share one stage.
   *
   * @return validated immutable binding
   */
  public CompletionStage<Binding> binding() {
    return operation(
        (CompletableFuture<Binding> result) -> {
          requireReady();
          if (binding != null) {
            result.complete(binding);
            return;
          }
          if (bindingStage != null) {
            bindingStage.whenComplete(
                (value, failure) -> {
                  if (failure != null) result.completeExceptionally(failure);
                  else result.complete(value);
                });
            return;
          }
          bindingStage = result;
          ClientJournal.Intent intent = journal.intent();
          Optional<Binding> retained = journal.binding();
          long id = allocate();
          Message request =
              retained
                  .<Message>map(b -> new Attach(id, b.authority(), b.owner(), b.generation()))
                  .orElseGet(() -> new Create(id, intent.creationSequence(), intent.policy()));
          send(
              request,
              null,
              result,
              response -> {
                if (response instanceof Refusal refusal) {
                  finishBinding(result, null, refused(refusal));
                  return;
                }
                Binding received = (Binding) response;
                blocking(
                    () -> {
                      if (!received.authority().equals(intent.authority())
                          || !received.owner().equals(intent.owner())
                          || received.creationSequence() != intent.creationSequence()
                          || !received.policy().equals(intent.policy()))
                        throw new ProtocolError(
                            INTEGRITY_ERROR, "binding contradicts journaled intent");
                      if (retained.isPresent()
                          && retained.get().generation() != received.generation())
                        throw new ProtocolError(INTEGRITY_ERROR, "attachment generation differs");
                      journal.bind(received);
                      return journal.binding().orElseThrow();
                    },
                    bound -> finishBinding(result, bound, null),
                    failure -> finishBinding(result, null, named(failure)));
              });
        });
  }

  private void finishBinding(CompletableFuture<Binding> result, Binding bound, Throwable failure) {
    bindingStage = null;
    if (failure != null) {
      result.completeExceptionally(failure);
      return;
    }
    binding = bound;
    result.complete(bound);
  }

  /**
   * Query the owner's next creation sequence without allocating a session.
   *
   * @return next sequence
   */
  public CompletionStage<Long> nextSequence() {
    return operation(
        (CompletableFuture<Long> result) -> {
          requireReady();
          send(
              new NextSequence(allocate()),
              null,
              result,
              response -> {
                if (response instanceof Refusal refusal)
                  result.completeExceptionally(refused(refusal));
                else result.complete(((Sequence) response).nextCreationSequence());
              });
        });
  }

  // ---------------------------------------------------------------- mutations

  private CompletionStage<Records.OperationReceipt> mutation(Message template) {
    return operation(
        (CompletableFuture<Records.OperationReceipt> result) -> {
          requireBound();
          Records.OperationId id = ClientJournal.operationOf(template);
          blocking(
              () -> {
                journal.journalMutation(template);
                boundaries.committed(
                    Boundaries.Boundary.INTENT_JOURNALED, Boundaries.Details.NONE.operation(id));
                Optional<Records.OperationReceipt> retained = journal.receipt(id);
                return retained;
              },
              retained -> {
                if (retained.isPresent()) {
                  result.complete(retained.get());
                  return;
                }
                if (stopping) {
                  result.completeExceptionally(new ProtocolError(CANCELLED, "client stopped"));
                  return;
                }
                Message request = ClientJournal.withRequest(template, allocate());
                send(
                    request,
                    null,
                    result,
                    response -> receipt(result, id, response),
                    Boundaries.Details.NONE.operation(id));
              },
              failure -> result.completeExceptionally(named(failure)));
        });
  }

  private void receipt(
      CompletableFuture<Records.OperationReceipt> result,
      Records.OperationId id,
      Message response) {
    if (response instanceof Refusal refusal) {
      result.completeExceptionally(refused(refusal));
      return;
    }
    Records.OperationReceipt receipt =
        switch (response) {
          case DeclarationResponse r -> r.receipt();
          case CancelScopeResponse r -> r.receipt();
          case RetryResponse r -> r.receipt();
          case CancelResponse r -> r.receipt();
          case SkipResponse r -> r.receipt();
          case OperationResponse r -> r.receipt();
          case AdmissionResponse r -> r.receipt();
          default -> null;
        };
    if (receipt == null) {
      result.completeExceptionally(ProtocolError.frame("response is not a receipt"));
      return;
    }
    blocking(
        () -> {
          ClientJournal.PendingOperation pending =
              journal
                  .operation(id)
                  .orElseThrow(
                      () -> new ProtocolError(CONFLICT, "receipt for unjournaled operation"));
          Records.Digest expected =
              pending.input() != null
                  ? Commitments.operation(context(), 0, pending.input())
                  : Commitments.operation(context(), 0, pending.mutation());
          ClientValidation.receipt(pending, expected, receipt);
          boundaries.committed(
              Boundaries.Boundary.RECEIPT_VALIDATED, Boundaries.Details.NONE.operation(id));
          journal.journalReceipt(receipt);
          boundaries.committed(
              Boundaries.Boundary.RECEIPT_JOURNALED, Boundaries.Details.NONE.operation(id));
          return receipt;
        },
        result::complete,
        failure -> result.completeExceptionally(named(failure)));
  }

  /**
   * Declare members in a scope, optionally sealing it.
   *
   * @param operation immutable operation identity
   * @param scope target scope
   * @param entities strictly increasing member identities, at most 256
   * @param seal whether to seal after this batch
   * @return validated declaration receipt
   */
  public CompletionStage<Records.OperationReceipt> declare(
      Records.OperationId operation, long scope, List<Long> entities, boolean seal) {
    return mutation(new Declare(1, operation, scope, entities, seal));
  }

  /**
   * Cancel a whole scope.
   *
   * @param operation immutable operation identity
   * @param scope target scope
   * @return validated receipt
   */
  public CompletionStage<Records.OperationReceipt> cancelScope(
      Records.OperationId operation, long scope) {
    return mutation(new CancelScope(1, operation, scope));
  }

  /**
   * Explicitly retry the current attempt.
   *
   * @param operation immutable operation identity
   * @param work target work
   * @param expectedAttempt current attempt
   * @return validated receipt
   */
  public CompletionStage<Records.OperationReceipt> retry(
      Records.OperationId operation, Records.WorkKey work, long expectedAttempt) {
    return mutation(new Retry(1, operation, work, expectedAttempt));
  }

  /**
   * Request cancellation.
   *
   * @param operation immutable operation identity
   * @param work target work
   * @return validated receipt
   */
  public CompletionStage<Records.OperationReceipt> cancel(
      Records.OperationId operation, Records.WorkKey work) {
    return mutation(new Cancel(1, operation, work));
  }

  /**
   * Request an explicit skip.
   *
   * @param operation immutable operation identity
   * @param work target work
   * @return validated receipt
   */
  public CompletionStage<Records.OperationReceipt> skip(
      Records.OperationId operation, Records.WorkKey work) {
    return mutation(new Skip(1, operation, work));
  }

  /**
   * Look up a journaled operation's retained receipt at the authority.
   *
   * @param operation journaled operation identity
   * @return validated receipt
   */
  public CompletionStage<Records.OperationReceipt> lookup(Records.OperationId operation) {
    return operation(
        (CompletableFuture<Records.OperationReceipt> result) -> {
          requireBound();
          blocking(
              () -> journal.operation(operation).isPresent(),
              journaled -> {
                if (!journaled) {
                  result.completeExceptionally(
                      new ProtocolError(NOT_FOUND, "operation not journaled"));
                  return;
                }
                send(
                    new LookupOperation(allocate(), operation),
                    null,
                    result,
                    response -> receipt(result, operation, response));
              },
              failure -> result.completeExceptionally(named(failure)));
        });
  }

  // ---------------------------------------------------------------- admission

  /**
   * Journal and stream one input admission from a stable handle. The stage completes with the
   * validated admission receipt; the source is closed when the transfer ends.
   *
   * @param operation immutable operation identity
   * @param parameters exact admission parameters; the input commitment must equal the source's
   * @param declaration covering declaration operation, already receipted
   * @param source opened, prehashed input
   * @return validated admission receipt
   */
  public CompletionStage<Records.OperationReceipt> admit(
      Records.OperationId operation,
      Records.AdmitParameters parameters,
      Records.OperationId declaration,
      InputSource source) {
    return operation(
        (CompletableFuture<Records.OperationReceipt> result) -> {
          requireBound();
          if (!parameters.input().equals(source.input()))
            throw new ProtocolError(INTEGRITY_ERROR, "parameters contradict the source commitment");
          Records.InputHeader header =
              new Records.InputHeader(binding.generation(), operation, parameters);
          blocking(
              () -> {
                journal.journalInput(header, declaration);
                boundaries.committed(
                    Boundaries.Boundary.INTENT_JOURNALED,
                    Boundaries.Details.NONE.operation(operation).work(parameters.work()));
                return journal.receipt(operation);
              },
              retained -> {
                if (retained.isPresent()) {
                  closeSource(source);
                  result.complete(retained.get());
                  return;
                }
                InputTransfer transfer = new InputTransfer(header, source, result);
                transfer.begin();
              },
              failure -> {
                closeSource(source);
                result.completeExceptionally(named(failure));
              });
        });
  }

  private void closeSource(InputSource source) {
    blocking(
        () -> {
          source.close();
          return null;
        },
        ignored -> {},
        ignored -> {});
  }

  /** One outgoing input stream: header, file chunks, FIN, then the correlated admission receipt. */
  private final class InputTransfer {
    final Records.InputHeader header;
    final InputSource source;
    final CompletableFuture<Records.OperationReceipt> result;
    final ByteBuffer buffer;
    StreamTransport.Data data;
    long streamId = -1;
    long offset;
    boolean finished;
    boolean done;

    InputTransfer(
        Records.InputHeader header,
        InputSource source,
        CompletableFuture<Records.OperationReceipt> result) {
      this.header = header;
      this.source = source;
      this.result = result;
      buffer = ByteBuffer.allocate(options.chunkBytes());
    }

    void begin() {
      CompletionStage<StreamTransport.Data> opening;
      try {
        opening = transport.openData(new ChannelInboundHandlerAdapter());
      } catch (ProtocolError failure) {
        end(failure);
        return;
      }
      opening.whenComplete(
          (opened, failure) ->
              onLoop(
                  () -> {
                    if (failure != null) {
                      end(named(failure));
                      return;
                    }
                    if (done) {
                      opened.abort(new ProtocolError(CANCELLED, "transfer ended"));
                      opened.release();
                      return;
                    }
                    data = opened;
                    streamId = opened.stream().streamId();
                    try {
                      correlation.registerInput(streamId, header);
                    } catch (ProtocolError refused) {
                      end(refused);
                      return;
                    }
                    inputTransfers.put(streamId, this);
                    writeHeader(Wire.encodeHeader(header));
                  }));
    }

    private void writeHeader(byte[] bytes) {
      if (done) return;
      int chunk = Math.min(bytes.length, options.chunkBytes());
      byte[] part = java.util.Arrays.copyOfRange(bytes, 0, chunk);
      byte[] rest = java.util.Arrays.copyOfRange(bytes, chunk, bytes.length);
      data.write(part)
          .whenComplete(
              (ignored, failure) -> {
                if (failure != null) {
                  transportFailed(failure);
                  return;
                }
                if (rest.length > 0) writeHeader(rest);
                else pump();
              });
    }

    private void pump() {
      if (done || finished) return;
      blocking(
          () -> {
            int read = source.read(offset, buffer);
            if (read < 0) return null;
            byte[] chunk = new byte[read];
            buffer.get(chunk);
            return chunk;
          },
          chunk -> {
            if (done) return;
            if (chunk == null) {
              finished = true;
              try {
                data.finish()
                    .whenComplete(
                        (ignored, failure) -> {
                          if (failure != null) transportFailed(failure);
                        });
              } catch (IllegalStateException | ProtocolError stopped) {
                transportFailed(stopped);
              }
              return;
            }
            offset += chunk.length;
            try {
              data.write(chunk)
                  .whenComplete(
                      (ignored, failure) -> {
                        if (failure != null) transportFailed(failure);
                        else pump();
                      });
            } catch (IllegalStateException | ProtocolError stopped) {
              transportFailed(stopped);
            }
          },
          failure -> end(named(failure)));
    }

    private void transportFailed(Throwable failure) {
      // A redundant replayed input is stopped by the authority with error 0; the correlated
      // admission response still arrives on the control stream, so wait for it.
      if (done) return;
      finished = true;
    }

    void response(Message message) {
      if (done) return;
      done = true;
      inputTransfers.remove(streamId);
      releaseData();
      if (message instanceof Refusal refusal) {
        boundaries.sent(
            Boundaries.Boundary.REFUSAL_RECEIVED,
            Boundaries.Details.NONE.operation(header.operation()).refusal(refusal.code()));
        closeSource(source);
        result.completeExceptionally(refused(refusal));
        return;
      }
      AdmissionResponse admission = (AdmissionResponse) message;
      blocking(
          () -> {
            ClientJournal.PendingOperation pending =
                journal
                    .operation(header.operation())
                    .orElseThrow(() -> new ProtocolError(CONFLICT, "unjournaled input"));
            ClientValidation.receipt(
                pending, Commitments.operation(context(), 0, header), admission.receipt());
            Boundaries.Details details =
                Boundaries.Details.NONE
                    .operation(header.operation())
                    .work(header.parameters().work());
            boundaries.committed(Boundaries.Boundary.RECEIPT_VALIDATED, details);
            journal.journalReceipt(admission.receipt());
            boundaries.committed(Boundaries.Boundary.RECEIPT_JOURNALED, details);
            source.close();
            return admission.receipt();
          },
          result::complete,
          failure -> result.completeExceptionally(named(failure)));
    }

    private void releaseData() {
      if (data == null) return;
      if (!finished) data.abort(new ProtocolError(CANCELLED, "input transfer ended"));
      try {
        data.release();
      } catch (IllegalStateException live) {
        data.abort(new ProtocolError(CANCELLED, "input transfer ended"));
        data.release();
      }
    }

    void end(ProtocolError failure) {
      if (done) return;
      done = true;
      if (streamId >= 0) inputTransfers.remove(streamId);
      releaseData();
      closeSource(source);
      result.completeExceptionally(failure);
    }

    void lost(Throwable cause) {
      if (done) return;
      done = true;
      closeSource(source);
      result.completeExceptionally(named(cause));
    }
  }

  // ---------------------------------------------------------------- observations

  /**
   * Observe a work view, optionally waiting for a revision change, validate it against every
   * journaled commitment and journal it.
   *
   * @param work logical work
   * @param afterRevision zero for an immediate snapshot
   * @param waitMs bounded wait, 0..30000
   * @return validated observation
   */
  public CompletionStage<ClientJournal.Observed> watch(
      Records.WorkKey work, long afterRevision, long waitMs) {
    return operation(
        (CompletableFuture<ClientJournal.Observed> result) -> {
          requireBound();
          send(
              new Watch(allocate(), work, afterRevision, waitMs),
              null,
              result,
              response -> {
                if (response instanceof Refusal refusal) {
                  result.completeExceptionally(refused(refusal));
                  return;
                }
                WatchResponse view = (WatchResponse) response;
                blocking(
                    () -> {
                      if (!view.work().work().equals(work))
                        throw new ProtocolError(INTEGRITY_ERROR, "view names other work");
                      view.work().validateProfiles(journal.intent().results());
                      relationships(view.work());
                      journal.observeWork(view.revision(), view.work());
                      boundaries.committed(
                          Boundaries.Boundary.OBSERVATION_JOURNALED,
                          Boundaries.Details.NONE.work(work));
                      return new ClientJournal.Observed(view.revision(), view.work());
                    },
                    result::complete,
                    failure -> result.completeExceptionally(named(failure)));
              });
        });
  }

  /**
   * Cross-check a view against retained scope/parent evidence in both directions. Worker thread.
   */
  private void relationships(Records.WorkView view) throws SQLException {
    Optional<ClientJournal.ScopeEvidence> own = journal.scope(view.work().scope());
    if (own.isPresent()) {
      boolean member =
          journal
              .members(view.work().scope(), view.work().entity() - 1, 1)
              .contains(view.work().entity());
      ClientValidation.membership(own.get(), view, member);
      if (own.get().parent() != null) {
        Optional<ClientJournal.Observed> parent = journal.observedWork(own.get().parent());
        if (parent.isPresent()) ClientValidation.relationship(parent.get().view(), own.get());
      }
    }
    if (view.child() != null) {
      Optional<ClientJournal.ScopeEvidence> child = journal.scope(view.child().scope());
      if (child.isPresent()) ClientValidation.relationship(view, child.get());
    }
  }

  /**
   * Page a scope's membership, validate ordering/identity and accumulate the seal.
   *
   * @param scope scope identity
   * @param afterEntity exclusive lower bound
   * @param limit maximum entries, 1..256
   * @return validated page
   */
  public CompletionStage<ScopePage> page(long scope, long afterEntity, int limit) {
    return operation(
        (CompletableFuture<ScopePage> result) -> {
          requireBound();
          send(
              new Page(allocate(), scope, afterEntity, limit),
              null,
              result,
              response -> {
                if (response instanceof Refusal refusal) {
                  result.completeExceptionally(refused(refusal));
                  return;
                }
                PageResponse page = (PageResponse) response;
                blocking(
                    () -> {
                      if (page.scope() != scope)
                        throw new ProtocolError(INTEGRITY_ERROR, "page names other scope");
                      long previous = afterEntity;
                      for (Entry entry : page.entries()) {
                        if (entry.entity() <= previous)
                          throw new ProtocolError(
                              INTEGRITY_ERROR, "page members not strictly increasing");
                        previous = entry.entity();
                      }
                      if (page.entries().size() > limit)
                        throw new ProtocolError(INTEGRITY_ERROR, "page exceeds limit");
                      if (page.sealed()
                          && page.seal() == null
                          && page.declared() > 0
                          && !page.more())
                        throw new ProtocolError(INTEGRITY_ERROR, "sealed page without seal");
                      journal.observePage(page);
                      boundaries.committed(
                          Boundaries.Boundary.OBSERVATION_JOURNALED, Boundaries.Details.NONE);
                      ClientJournal.ScopeEvidence evidence = journal.scope(scope).orElseThrow();
                      if (evidence.parent() != null) {
                        Optional<ClientJournal.Observed> parent =
                            journal.observedWork(evidence.parent());
                        if (parent.isPresent())
                          ClientValidation.relationship(parent.get().view(), evidence);
                      }
                      for (Entry entry : page.entries()) {
                        Optional<ClientJournal.Observed> member =
                            journal.observedWork(
                                new Records.WorkKey(scope, page.producer(), entry.entity()));
                        if (member.isPresent())
                          ClientValidation.membership(evidence, member.get().view(), true);
                      }
                      return new ScopePage(
                          scope,
                          page.producer(),
                          page.parent(),
                          page.declared(),
                          page.sealed(),
                          page.seal(),
                          page.entries(),
                          page.more(),
                          evidence.membershipVerified());
                    },
                    result::complete,
                    failure -> result.completeExceptionally(named(failure)));
              });
        });
  }

  /**
   * Request a checkpoint over a verified seal, validate the summary against the complete retained
   * bottom-up evidence and journal it.
   *
   * @param scope scope identity
   * @param seal expected seal
   * @param waitMs bounded wait, 0..30000
   * @return validated immutable summary
   */
  public CompletionStage<Records.ScopeSummary> checkpoint(
      long scope, Records.Digest seal, long waitMs) {
    return operation(
        (CompletableFuture<Records.ScopeSummary> result) -> {
          requireBound();
          blocking(
              () -> {
                ClientJournal.ScopeEvidence evidence =
                    journal
                        .scope(scope)
                        .orElseThrow(
                            () -> new ProtocolError(NOT_READY, "scope membership not observed"));
                if (!evidence.membershipVerified()
                    || evidence.seal() == null
                    || !evidence.seal().equals(seal))
                  throw new ProtocolError(
                      NOT_READY, "sealed membership not verified for this seal");
                return evidence;
              },
              evidence -> {
                if (evidence.summary() != null) {
                  result.complete(evidence.summary());
                  return;
                }
                send(
                    new Checkpoint(allocate(), scope, seal, waitMs),
                    null,
                    result,
                    response -> {
                      if (response instanceof Refusal refusal) {
                        result.completeExceptionally(refused(refusal));
                        return;
                      }
                      Records.ScopeSummary summary = ((CheckpointResponse) response).summary();
                      blocking(
                          () -> {
                            ClientValidation.summary(evidence, summary);
                            Commitments.Status computed = statusRoot(evidence);
                            if (!computed.root().equals(summary.statusRoot())
                                || !computed.counts().equals(summary.counts()))
                              throw new ProtocolError(
                                  INTEGRITY_ERROR, "summary contradicts retained member evidence");
                            journal.observeSummary(summary);
                            boundaries.committed(
                                Boundaries.Boundary.OBSERVATION_JOURNALED, Boundaries.Details.NONE);
                            return summary;
                          },
                          result::complete,
                          failure -> result.completeExceptionally(named(failure)));
                    });
              },
              failure -> result.completeExceptionally(named(failure)));
        });
  }

  /** Recompute a scope's status root from complete retained terminal member evidence. Worker. */
  private Commitments.Status statusRoot(ClientJournal.ScopeEvidence evidence) throws SQLException {
    Commitments.StatusTree tree =
        new Commitments.StatusTree(evidence.scope(), evidence.producer(), evidence.declared());
    long after = 0;
    while (true) {
      List<Long> members = journal.members(evidence.scope(), after, 256);
      if (members.isEmpty()) break;
      for (long entity : members) {
        Records.WorkKey key = new Records.WorkKey(evidence.scope(), evidence.producer(), entity);
        Records.WorkView view =
            journal
                .observedWork(key)
                .orElseThrow(() -> new ProtocolError(NOT_READY, "member evidence missing: " + key))
                .view();
        if (!view.state().terminal())
          throw new ProtocolError(
              NOT_READY, "member not yet terminal in retained evidence: " + key);
        Records.Digest childRoot = null;
        if (view.child() != null) {
          ClientJournal.ScopeEvidence child =
              journal
                  .scope(view.child().scope())
                  .orElseThrow(() -> new ProtocolError(NOT_READY, "child scope evidence missing"));
          if (child.summary() == null)
            throw new ProtocolError(NOT_READY, "child scope summary not yet verified");
          childRoot = child.summary().statusRoot();
        }
        tree.add(view, childRoot);
        after = entity;
      }
    }
    return tree.finish();
  }

  /**
   * Fetch, validate and journal a retained manifest.
   *
   * @param work logical work
   * @param attempt producing attempt
   * @return validated manifest
   */
  public CompletionStage<Records.Manifest> manifest(Records.WorkKey work, long attempt) {
    return operation(
        (CompletableFuture<Records.Manifest> result) -> {
          requireBound();
          send(
              new GetManifest(allocate(), work, attempt),
              null,
              result,
              response -> {
                if (response instanceof Refusal refusal) {
                  result.completeExceptionally(refused(refusal));
                  return;
                }
                Records.Manifest manifest = ((ManifestResponse) response).manifest();
                blocking(
                    () -> {
                      if (!manifest.work().equals(work) || manifest.attempt() != attempt)
                        throw new ProtocolError(INTEGRITY_ERROR, "manifest names other work");
                      ClientValidation.manifest(
                          context(),
                          journal.observedWork(work).map(ClientJournal.Observed::view).orElse(null),
                          manifest);
                      journal.observeManifest(manifest);
                      boundaries.committed(
                          Boundaries.Boundary.OBSERVATION_JOURNALED,
                          Boundaries.Details.NONE.work(work).attempt(attempt));
                      return manifest;
                    },
                    result::complete,
                    failure -> result.completeExceptionally(named(failure)));
              });
        });
  }

  /**
   * Save an explicit output selection from a retained manifest.
   *
   * @param work logical work
   * @param attempt producing attempt
   * @param index output index
   * @return selection
   */
  public CompletionStage<ClientJournal.Selection> select(
      Records.WorkKey work, long attempt, int index) {
    return operation(
        (CompletableFuture<ClientJournal.Selection> result) ->
            blocking(
                () -> journal.select(work, attempt, index),
                result::complete,
                failure -> result.completeExceptionally(named(failure))));
  }

  /**
   * Read one selected output through a fresh authorized transfer, verifying header, bytes, length,
   * digest and FIN before installing the destination.
   *
   * @param work logical work
   * @param attempt producing attempt
   * @param index output index
   * @param destination new destination file
   * @return verified delivery
   */
  public CompletionStage<ResultFiles.Delivered> read(
      Records.WorkKey work, long attempt, int index, ResultFiles.Destination destination) {
    return operation(
        (CompletableFuture<ResultFiles.Delivered> result) -> {
          requireBound();
          if (!selected.supported().contains(RESULT_DELIVERY))
            throw new ProtocolError(EXTENSION_UNSUPPORTED, "result delivery not selected");
          blocking(
              () -> {
                ClientJournal.Selection selection =
                    journal
                        .selection(work, attempt, index)
                        .orElseThrow(
                            () ->
                                new ProtocolError(
                                    NOT_FOUND, "no saved selection; call select first"));
                return new Object[] {selection, new ResultFiles.Staging(destination)};
              },
              prepared -> {
                ClientJournal.Selection selection = (ClientJournal.Selection) prepared[0];
                ResultFiles.Staging staging = (ResultFiles.Staging) prepared[1];
                long id = allocate();
                ResultTransfer transfer = new ResultTransfer(id, selection, staging, result);
                try {
                  send(
                      new Read(id, work, attempt, index, selection.output().sha256()),
                      ClientCorrelation.ResultCommitment.from(selection.manifest(), index),
                      result,
                      transfer::controlResponse);
                } catch (ProtocolError failure) {
                  transfer.lost(failure);
                  return;
                }
                resultTransfers.put(id, transfer);
              },
              failure -> result.completeExceptionally(named(failure)));
        });
  }

  /** One incoming result stream bound to a pending read. */
  private final class ResultTransfer extends SimpleChannelInboundHandler<QuicStreamFrame> {
    final long request;
    final ClientJournal.Selection selection;
    final ResultFiles.Staging staging;
    final CompletableFuture<ResultFiles.Delivered> result;
    final long accepted = System.nanoTime();
    ObjectStream.HeaderReader headerReader;
    StreamTransport.Data data;
    QuicStreamChannel stream;
    boolean headerDone;
    boolean done;
    boolean busy;

    ResultTransfer(
        long request,
        ClientJournal.Selection selection,
        ResultFiles.Staging staging,
        CompletableFuture<ResultFiles.Delivered> result) {
      this.request = request;
      this.selection = selection;
      this.staging = staging;
      this.result = result;
    }

    void controlResponse(Message message) {
      if (message instanceof Refusal refusal) end(refused(refusal));
      else end(ProtocolError.frame("read completed without a result stream"));
    }

    void check(long now) {
      if (done) return;
      try {
        if (!headerDone) headerReader.checkDeadline(now);
        else correlation.checkResultDeadline(request, now);
      } catch (ProtocolError expired) {
        abort(expired);
      }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, QuicStreamFrame frame) {
      if (done) return;
      ByteBuf content = frame.content();
      boolean fin = frame.hasFin();
      try {
        if (!headerDone) throw ProtocolError.frame("payload before header binding");
        byte[] chunk = new byte[content.readableBytes()];
        content.readBytes(chunk);
        long now = System.nanoTime();
        correlation.resultBytes(request, ByteBuffer.wrap(chunk), now);
        busy = true;
        blocking(
            () -> {
              staging.write(ByteBuffer.wrap(chunk));
              return null;
            },
            ignored -> {
              busy = false;
              if (done) return;
              if (fin) finish();
              else stream.read();
            },
            failure -> abort(named(failure)));
      } catch (ProtocolError failure) {
        abort(failure);
      }
    }

    private void finish() {
      long now = System.nanoTime();
      try {
        correlation.finishResult(request, now);
      } catch (ProtocolError failure) {
        abort(failure);
        return;
      }
      done = true;
      resultTransfers.remove(request);
      controls.remove(request);
      incomingResults.remove(this);
      data.release();
      stream.close();
      Records.Output output = selection.output();
      Boundaries.Details details =
          Boundaries.Details.NONE
              .work(selection.manifest().work())
              .attempt(selection.manifest().attempt());
      blocking(
          () -> {
            staging.verify(output.length(), output.sha256());
            boundaries.committed(Boundaries.Boundary.RESULT_VERIFIED, details);
            ResultFiles.Delivered delivered = staging.install(output.length(), output.sha256());
            boundaries.committed(Boundaries.Boundary.RESULT_INSTALLED, details);
            return delivered;
          },
          result::complete,
          failure -> {
            closeStaging();
            result.completeExceptionally(named(failure));
          });
    }

    private void closeStaging() {
      blocking(
          () -> {
            staging.close();
            return null;
          },
          ignored -> {},
          ignored -> {});
    }

    void abort(ProtocolError failure) {
      if (done) return;
      done = true;
      resultTransfers.remove(request);
      controls.remove(request);
      incomingResults.remove(this);
      if (headerDone) {
        try {
          correlation.abortResult(request);
        } catch (ProtocolError ignored) {
          // Already released.
        }
      }
      if (data != null) {
        data.abort(failure);
        data.release();
        if (stream != null) stream.close();
      }
      closeStaging();
      result.completeExceptionally(failure);
    }

    void end(ProtocolError failure) {
      abort(failure);
    }

    void lost(Throwable cause) {
      if (done) return;
      done = true;
      resultTransfers.remove(request);
      controls.remove(request);
      incomingResults.remove(this);
      closeStaging();
      result.completeExceptionally(named(cause));
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      if (!done) abort(new ProtocolError(CONTROL_RESET, "result stream ended before FIN"));
      ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      abort(new ProtocolError(CONTROL_RESET, "result stream reset"));
    }
  }

  /** A server-initiated unidirectional stream: read the header, bind it to a pending read. */
  private void incomingStream(QuicStreamChannel stream) {
    if (stopping) {
      stream.close();
      return;
    }
    try {
      if (selected == null) throw ProtocolError.frame("object stream before negotiation");
      StreamTransport.Data data = transport.claimIncoming(stream);
      ObjectStream.HeaderReader reader =
          new ObjectStream.HeaderReader(false, System.nanoTime(), options.headerTimeoutMs());
      stream
          .pipeline()
          .addLast(
              new SimpleChannelInboundHandler<QuicStreamFrame>() {
                ResultTransfer bound;

                @Override
                protected void channelRead0(ChannelHandlerContext ctx, QuicStreamFrame frame) {
                  if (stopping) return;
                  try {
                    if (bound != null) {
                      ctx.fireChannelRead(frame.retain());
                      return;
                    }
                    ByteBuffer source = frame.content().nioBuffer();
                    Records.Value decoded = reader.feed(source, System.nanoTime());
                    if (decoded == null) {
                      if (frame.hasFin()) reader.finish(System.nanoTime());
                      stream.read();
                      return;
                    }
                    Records.ResultHeader header = (Records.ResultHeader) decoded;
                    long now = System.nanoTime();
                    // Unknown/duplicate correlation is fatal; a contradiction is delivery-local.
                    ResultTransfer transfer = resultTransfers.get(header.request());
                    try {
                      correlation.beginResult(header, now);
                    } catch (ProtocolError failure) {
                      if (failure.code() != INTEGRITY_ERROR || transfer == null) throw failure;
                      data.abort(failure);
                      data.release();
                      transfer.abort(failure);
                      return;
                    }
                    if (transfer == null) throw ProtocolError.frame("result for unknown read");
                    transfer.headerReader = reader;
                    transfer.headerDone = true;
                    bound = transfer;
                    transfer.stream = stream;
                    transfer.data = data;
                    incomingResults.add(transfer);
                    stream.pipeline().addLast(transfer);
                    byte[] rest = new byte[source.remaining()];
                    source.get(rest);
                    if (rest.length > 0 || frame.hasFin()) {
                      QuicStreamFrame tail =
                          new io.netty.handler.codec.quic.DefaultQuicStreamFrame(
                              Unpooled.wrappedBuffer(rest), frame.hasFin());
                      ctx.fireChannelRead(tail);
                    } else stream.read();
                  } catch (ProtocolError failure) {
                    fail(failure);
                  }
                }

                @Override
                public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                  if (bound == null) {
                    data.abort(new ProtocolError(CONTROL_RESET, "result stream reset"));
                    data.release();
                  } else ctx.fireExceptionCaught(cause);
                }

                @Override
                public void channelInactive(ChannelHandlerContext ctx) {
                  if (bound == null && !stopping) {
                    data.abort(new ProtocolError(CONTROL_RESET, "result stream ended"));
                    data.release();
                  }
                  ctx.fireChannelInactive();
                }
              });
      stream.read();
    } catch (ProtocolError failure) {
      fail(failure);
    }
  }

  // ---------------------------------------------------------------- completion and detach

  /**
   * Request completed-session shutdown with the exact journaled root summary.
   *
   * @return acknowledged root summary
   */
  public CompletionStage<Records.ScopeSummary> complete() {
    return operation(
        (CompletableFuture<Records.ScopeSummary> result) -> {
          requireBound();
          blocking(
              () ->
                  journal
                      .scope(0)
                      .map(ClientJournal.ScopeEvidence::summary)
                      .orElseThrow(
                          () -> new ProtocolError(NOT_READY, "root summary not yet verified")),
              root -> {
                if (root == null) {
                  result.completeExceptionally(
                      new ProtocolError(NOT_READY, "root summary not yet verified"));
                  return;
                }
                send(
                    new Complete(allocate(), binding.generation(), root),
                    null,
                    result,
                    response -> {
                      if (response instanceof Refusal refusal) {
                        result.completeExceptionally(refused(refusal));
                        return;
                      }
                      Completed completed = (Completed) response;
                      blocking(
                          () -> {
                            if (completed.generation() != binding.generation()
                                || !completed.root().equals(root))
                              throw new ProtocolError(
                                  INTEGRITY_ERROR, "completion echoes another root");
                            journal.completed(root);
                            return root;
                          },
                          result::complete,
                          failure -> result.completeExceptionally(named(failure)));
                    });
              },
              failure -> result.completeExceptionally(named(failure)));
        });
  }

  /**
   * Request connection-only detach. Success requires the correlated acknowledgment, the server's
   * control FIN and local FIN completion; it asserts nothing about durable work.
   *
   * @return drain result
   */
  public CompletionStage<Void> detach() {
    loop.execute(
        () -> {
          if (stopping || detachSent) return;
          try {
            requireReady();
            detachSent = true;
            detachStart = System.nanoTime();
            long id = allocate();
            send(
                new Detach(id),
                null,
                detached,
                response -> {
                  if (response instanceof Refusal refusal) fail(refused(refusal));
                  else {
                    detachAcknowledged = true;
                    control.finish();
                  }
                });
            control.finishOutput();
          } catch (ProtocolError failure) {
            fail(failure);
          }
        });
    return detached.minimalCompletionStage();
  }

  // ---------------------------------------------------------------- transport handlers

  private final class Connection extends ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
      if (stopping) return;
      try {
        check();
        if (stopping) return;
        guard.requireAuthenticated();
        authenticated = true;
        lastFrame = System.nanoTime();
        connection
            .createStream(
                QuicStreamType.BIDIRECTIONAL,
                new ChannelInitializer<QuicStreamChannel>() {
                  @Override
                  protected void initChannel(QuicStreamChannel stream) {
                    transport.bindControl(stream);
                    stream
                        .config()
                        .setAllowHalfClosure(true)
                        .setRecvByteBufAllocator(
                            new FixedRecvByteBufAllocator(core.readChunkBytes()));
                    control = new Control(stream);
                    stream.pipeline().addLast(control);
                  }
                })
            .addListener(
                opened -> {
                  if (stopping) return;
                  if (!opened.isSuccess())
                    fail(new ProtocolError(CONTROL_RESET, "control open failed"));
                  else if (control.stream.streamId() != 0)
                    fail(ProtocolError.frame("control must use stream zero"));
                  else control.writes.send(offer);
                });
      } catch (ProtocolError failure) {
        fail(failure);
      }
      ctx.fireChannelActive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
      if (!stopping && event instanceof QuicConnectionCloseEvent closed) {
        if (closed.isApplicationClose() && closed.error() > 0x200 && closed.error() <= 0x212)
          fail(
              ProtocolError.refused(
                  ProtocolError.Code.from(closed.error() - 0x200L), "peer closed connection"));
        else
          fail(
              new IOException(
                  "peer closed before verified drain: application="
                      + closed.isApplicationClose()
                      + ", code="
                      + Integer.toUnsignedString(closed.error())));
      }
      ctx.fireUserEventTriggered(event);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      deferFailure(cause);
    }
  }

  private final class Control extends SimpleChannelInboundHandler<ByteBuf> {
    final QuicStreamChannel stream;
    final Wire.Decoder decoder = new Wire.Decoder(4096);
    final ControlWrites writes;
    boolean inputFin;
    boolean outputFinStarted;
    boolean outputFin;

    Control(QuicStreamChannel stream) {
      this.stream = stream;
      writes = new ControlWrites(stream, core, DurableClient.this::fail, this::finishOutput);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf bytes) {
      if (stopping) return;
      try {
        check();
        if (stopping) return;
        ByteBuffer source = bytes.nioBuffer();
        while (source.hasRemaining() && !stopping) {
          Wire.Frame frame = decoder.feed(source);
          if (frame == null) break;
          guard.requireAuthenticated();
          ClientCorrelation.Completion completion = correlation.receive(frame);
          lastFrame = System.nanoTime();
          if (selected == null) {
            Capabilities response = (Capabilities) ((Wire.Known) frame).message();
            if (!response.supported().equals(journal.intent().profiles()))
              throw new ProtocolError(
                  EXTENSION_UNSUPPORTED, "selection differs from journaled profiles");
            selected = response;
            decoder.limit(selected.controlLimit());
            writes.selected(selected);
            readiness.complete(selected);
            continue;
          }
          if (completion == null) continue;
          Records.RequestTag tag = completion.request().tag();
          if (tag.input()) {
            InputTransfer transfer = inputTransfers.get(tag.id());
            if (transfer != null) transfer.response(completion.response());
          } else {
            Continuation continuation = controls.remove(tag.id());
            if (continuation != null && completion.response() instanceof Refusal refusal)
              boundaries.sent(
                  Boundaries.Boundary.REFUSAL_RECEIVED,
                  Boundaries.Details.NONE.refusal(refusal.code()));
            if (continuation != null) continuation.response.accept(completion.response());
          }
        }
      } catch (ProtocolError failure) {
        fail(failure);
      }
    }

    void finishOutput() {
      if (!stopping
          && detachSent
          && !outputFinStarted
          && writes.empty()
          && controls.isEmpty()
          && inputTransfers.isEmpty()
          && resultTransfers.isEmpty()) {
        outputFinStarted = true;
        stream
            .shutdownOutput()
            .addListener(
                result -> {
                  if (!result.isSuccess())
                    fail(new ProtocolError(CONTROL_RESET, "control FIN failed"));
                  else {
                    outputFin = true;
                    finish();
                  }
                });
      }
    }

    void finish() {
      if (stopping) return;
      finishOutput();
      if (detachAcknowledged && inputFin && outputFin && writes.empty()) stop(null);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
      if (!stopping && event instanceof ChannelInputShutdownEvent) {
        try {
          decoder.finish();
          if (!detachAcknowledged)
            throw ProtocolError.frame("server FIN before detach acknowledgment");
          inputFin = true;
          finish();
        } catch (ProtocolError failure) {
          fail(failure);
        }
      } else ctx.fireUserEventTriggered(event);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      if (!stopping && !(inputFin && outputFin))
        fail(new ProtocolError(CONTROL_RESET, "control ended before verified drain"));
      ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      fail(new ProtocolError(CONTROL_RESET, "control stream reset or stopped"));
    }
  }
}
