package my.app.microbucketj.client.netty;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import lombok.extern.slf4j.Slf4j;
import my.app.microbucketj.client.model.AcquireResult;
import my.app.microbucketj.client.model.AcquireStatus;
import my.app.microbucketj.client.model.BucketGetResult;
import my.app.microbucketj.client.model.BucketType;
import my.app.microbucketj.client.model.BucketUpsertRequest;
import my.app.microbucketj.client.model.BucketUpsertResult;
import my.app.microbucketj.client.model.LeaderHint;
import my.app.microbucketj.client.model.ReportResult;
import my.app.microbucketj.client.protocol.ApiProtocol;
import my.app.microbucketj.client.protocol.ProtocolVersion;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@SuppressWarnings({"checkstyle:AbbreviationAsWordInName"})
@SuppressFBWarnings("URF_UNREAD_FIELD")
public final class NettyMicroBucketJTransport implements AutoCloseable {

  private static final AcquireResult ACQUIRE_UNAVAILABLE = new AcquireResult(AcquireStatus.UNAVAILABLE, 0, 0);
  private static final AcquireResult ACQUIRE_NOT_FOUND = new AcquireResult(AcquireStatus.NOT_FOUND, 0, 0);
  private static final ReportResult REPORT_UNAVAILABLE = ReportResult.unavailableResult();
  private static final ReportResult REPORT_NOT_FOUND = new ReportResult(ApiProtocol.ST_NOT_FOUND);

  private final List<InetSocketAddress> endpoints;
  private final Duration requestTimeout;
  private final int maxFrameSize;

  private final Connection[] pool;
  private final ConcurrentHashMap<String, byte[]> bucketNameBytesCache = new ConcurrentHashMap<>();

  private final AtomicInteger endpointRr = new AtomicInteger();
  private final AtomicInteger poolRr = new AtomicInteger();

  private final EventLoopGroup group;
  private final Bootstrap bootstrap;

  private volatile boolean closed;

  public NettyMicroBucketJTransport(
      List<InetSocketAddress> endpoints,
      Duration connectTimeout,
      Duration requestTimeout,
      int maxFrameSize,
      int ioThreads,
      int connectionPoolSize,
      boolean eagerConnect
  ) {
    this.endpoints = List.copyOf(Objects.requireNonNull(endpoints, "endpoints"));
    this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    this.maxFrameSize = maxFrameSize;

    int poolSize = Math.max(1, connectionPoolSize);
    this.pool = new Connection[poolSize];
    for (int i = 0; i < poolSize; i++) {
      this.pool[i] = new Connection(i);
    }
    boolean epoll = Epoll.isAvailable();
    this.group = epoll ? new EpollEventLoopGroup(Math.max(1, ioThreads)) : new NioEventLoopGroup(Math.max(1, ioThreads));

    Class<? extends Channel> ch = epoll ? EpollSocketChannel.class : NioSocketChannel.class;
    Duration connectTimeoutResult = Objects.requireNonNull(connectTimeout, "connectTimeout");
    this.bootstrap = new Bootstrap()
        .group(group)
        .channel(ch)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectTimeoutResult.toMillis())
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    if (eagerConnect) {
      for (Connection c : pool) {
        ensureConnectedAsync(c);
      }
    }
  }

  public EventLoopGroup group() {
    return group;
  }

  public int endpointCount() {
    return endpoints.size();
  }

  public CompletableFuture<AcquireResult> acquireByHash(long bucketHash, long permits) {
    if (closed) {
      return CompletableFuture.completedFuture(ACQUIRE_UNAVAILABLE);
    }
    long finalPermits = permits <= 0 ? 1 : permits;
    return send(RespKind.ACQUIRE, null, null, (ch, reqId) -> {
      ByteBuf out = ch.alloc().ioBuffer(64);
      out.writeByte(ApiProtocol.OP_ACQUIRE_HASH);
      out.writeInt(reqId);
      out.writeLong(finalPermits);
      out.writeLong(bucketHash);
      return out;
    }).handle((resp, err) -> {
      if (err != null || resp == null) {
        return ACQUIRE_UNAVAILABLE;
      }
      return (AcquireResult) resp;
    });
  }

  public CompletableFuture<AcquireResult> acquireByName(String bucket, long permits) {
    if (closed) {
      return CompletableFuture.completedFuture(ACQUIRE_UNAVAILABLE);
    }
    byte[] nameBytes = requireBucketNameBytes(bucket);
    if (nameBytes == null || nameBytes.length == 0) {
      return CompletableFuture.completedFuture(ACQUIRE_NOT_FOUND);
    }

    long finalPermits = permits <= 0 ? 1 : permits;
    return send(RespKind.ACQUIRE, bucket, null, (ch, reqId) -> {
      ByteBuf out = ch.alloc().ioBuffer(64 + nameBytes.length);
      out.writeByte(ApiProtocol.OP_ACQUIRE_NAME);
      out.writeInt(reqId);
      out.writeLong(finalPermits);
      writeU8(out, nameBytes);
      return out;
    }).handle((resp, err) -> {
      if (err != null || resp == null) {
        return ACQUIRE_UNAVAILABLE;
      }
      return (AcquireResult) resp;
    });
  }

  public CompletableFuture<ReportResult> reportByHash(long bucketHash, boolean success) {
    if (closed) {
      return CompletableFuture.completedFuture(REPORT_UNAVAILABLE);
    }
    return send(RespKind.REPORT, null, null, (ch, reqId) -> {
      ByteBuf out = ch.alloc().ioBuffer(32);
      out.writeByte(ApiProtocol.OP_REPORT_HASH);
      out.writeInt(reqId);
      out.writeByte(success ? 1 : 0);
      out.writeLong(bucketHash);
      return out;
    }).handle((resp, err) -> {
      if (err != null || resp == null) {
        return REPORT_UNAVAILABLE;
      }
      return (ReportResult) resp;
    });
  }

  public CompletableFuture<ReportResult> reportByName(String bucket, boolean success) {
    if (closed) {
      return CompletableFuture.completedFuture(REPORT_UNAVAILABLE);
    }
    byte[] nameBytes = requireBucketNameBytes(bucket);
    if (nameBytes == null || nameBytes.length == 0) {
      return CompletableFuture.completedFuture(REPORT_NOT_FOUND);
    }

    return send(RespKind.REPORT, bucket, null, (ch, reqId) -> {
      ByteBuf out = ch.alloc().ioBuffer(32 + nameBytes.length);
      out.writeByte(ApiProtocol.OP_REPORT_NAME);
      out.writeInt(reqId);
      out.writeByte(success ? 1 : 0);
      writeU8(out, nameBytes);
      return out;
    }).handle((resp, err) -> {
      if (err != null || resp == null) {
        return REPORT_UNAVAILABLE;
      }
      return (ReportResult) resp;
    });
  }

  public CompletableFuture<BucketUpsertResult> upsert(BucketUpsertRequest req) {
    if (closed) {
      return CompletableFuture.completedFuture(BucketUpsertResult.unavailable(req != null ? req.getBucket() : ""));
    }
    Objects.requireNonNull(req, "req");

    final String bucket = req.getBucket();
    byte[] nameBytes = requireBucketNameBytes(bucket);
    if (nameBytes == null || nameBytes.length == 0) {
      return CompletableFuture.completedFuture(BucketUpsertResult.unavailable(bucket));
    }

    return send(RespKind.UPSERT, bucket, req, (ch, reqId) -> {
      ByteBuf out = ch.alloc().ioBuffer(192 + nameBytes.length);
      out.writeByte(ApiProtocol.OP_BUCKET_UPSERT);
      out.writeInt(reqId);
      writeU8(out, nameBytes);
      writeVersionedSpecWithoutName(out, req);
      return out;
    }).handle((resp, err) -> {
      if (err != null || resp == null) {
        return BucketUpsertResult.unavailable(bucket);
      }
      return (BucketUpsertResult) resp;
    });
  }

  public CompletableFuture<BucketUpsertResult> upsertFollowingRedirects(BucketUpsertRequest req) {
    return upsert(req).thenCompose(result -> {
      if (result == null || !result.isRedirect() || endpoints.isEmpty()) {
        return CompletableFuture.completedFuture(result);
      }
      return upsertAcrossEndpoints(req, 0, result);
    });
  }

  private CompletableFuture<BucketUpsertResult> upsertAcrossEndpoints(BucketUpsertRequest req, int index, BucketUpsertResult lastResult) {
    if (index >= endpoints.size()) {
      return CompletableFuture.completedFuture(lastResult != null ? lastResult : BucketUpsertResult.unavailable(req.getBucket()));
    }
    InetSocketAddress endpoint = endpoints.get(index);
    byte[] nameBytes = requireBucketNameBytes(req.getBucket());
    if (nameBytes == null || nameBytes.length == 0) {
      return CompletableFuture.completedFuture(BucketUpsertResult.unavailable(req.getBucket()));
    }
    return sendSingle(endpoint, req.getBucket(), req, (ch, reqId) -> {
      ByteBuf out = ch.alloc().ioBuffer(192 + nameBytes.length);
      out.writeByte(ApiProtocol.OP_BUCKET_UPSERT);
      out.writeInt(reqId);
      writeU8(out, nameBytes);
      writeVersionedSpecWithoutName(out, req);
      return out;
    }).handle((resp, err) -> {
      if (err != null || resp == null) {
        return BucketUpsertResult.unavailable(req.getBucket());
      }
      return (BucketUpsertResult) resp;
    }).thenCompose(result -> {
      if (result.isOk() || result.isBadRequest()) {
        return CompletableFuture.completedFuture(result);
      }
      return upsertAcrossEndpoints(req, index + 1, result);
    });
  }

  public CompletableFuture<BucketGetResult> get(String bucket) {
    if (closed) {
      return CompletableFuture.completedFuture(BucketGetResult.unavailable(bucket != null ? bucket : ""));
    }
    byte[] nameBytes = requireBucketNameBytes(bucket);
    if (nameBytes == null || nameBytes.length == 0) {
      return CompletableFuture.completedFuture(BucketGetResult.unavailable(bucket));
    }

    return send(RespKind.GET, bucket, null, (ch, reqId) -> {
      ByteBuf out = ch.alloc().ioBuffer(64 + nameBytes.length);
      out.writeByte(ApiProtocol.OP_BUCKET_GET);
      out.writeInt(reqId);
      writeU8(out, nameBytes);
      return out;
    }).handle((resp, err) -> {
      if (err != null || resp == null) {
        return BucketGetResult.unavailable(bucket);
      }
      return (BucketGetResult) resp;
    });
  }

  private CompletableFuture<Object> send(RespKind kind, String bucket, BucketUpsertRequest upsertReq, PayloadEncoder encoder) {
    Connection c = pickConnection();
    if (c == null) {
      return CompletableFuture.failedFuture(new IllegalStateException("no connections"));
    }

    int reqId = c.nextRequestId();
    CompletableFuture<Object> out = new CompletableFuture<>();
    c.pending.put(reqId, new Pending(kind, bucket, upsertReq, out));

    CompletableFuture<Channel> chFut = ensureConnectedAsync(c);
    return chFut.handle((ch, connErr) -> {
      if (connErr != null || ch == null || !ch.isActive()) {
        Pending p = c.pending.remove(reqId);
        if (p != null) {
          p.future.completeExceptionally(connErr != null ? connErr : new IllegalStateException("unavailable"));
        }
        return null;
      }

      ScheduledFuture<?> timeout = ch.eventLoop().schedule(() -> {
        Pending p = c.pending.remove(reqId);
        if (p != null) {
          p.future.completeExceptionally(new TimeoutException("request timeout"));
        }
      }, Math.max(1L, requestTimeout.toMillis()), TimeUnit.MILLISECONDS);
      out.whenComplete((r, e) -> timeout.cancel(false));

      try {
        ByteBuf payload = encoder.encode(ch, reqId);
        ch.writeAndFlush(payload).addListener((ChannelFutureListener) f -> {
          if (!f.isSuccess()) {
            Pending p = c.pending.remove(reqId);
            if (p != null) {
              p.future.completeExceptionally(f.cause());
            }
          }
        });
      } catch (Throwable t) {
        Pending p = c.pending.remove(reqId);
        if (p != null) {
          p.future.completeExceptionally(t);
        }
      }

      return null;
    }).thenCompose(ignored -> out);
  }

  private Connection pickConnection() {
    if (pool.length == 0) {
      return null;
    }
    int idx = Math.floorMod(poolRr.getAndIncrement(), pool.length);
    return pool[idx];
  }

  private CompletableFuture<Channel> ensureConnectedAsync(Connection c) {
    if (closed) {
      return CompletableFuture.failedFuture(new IllegalStateException("transport closed"));
    }

    Channel ch = c.channel;
    if (ch != null && ch.isActive()) {
      return CompletableFuture.completedFuture(ch);
    }

    CompletableFuture<Channel> inFlight = c.connecting;
    if (inFlight != null && !inFlight.isDone()) {
      return inFlight;
    }

    synchronized (c.connectLock) {
      ch = c.channel;
      if (ch != null && ch.isActive()) {
        return CompletableFuture.completedFuture(ch);
      }

      inFlight = c.connecting;
      if (inFlight != null && !inFlight.isDone()) {
        return inFlight;
      }

      CompletableFuture<Channel> cf = new CompletableFuture<>();
      c.connecting = cf;
      connectAttempt(c, startEndpointIndex(), 0, cf);
      return cf;
    }
  }

  private int startEndpointIndex() {
    if (endpoints.isEmpty()) {
      return 0;
    }
    return Math.floorMod(endpointRr.getAndIncrement(), endpoints.size());
  }

  private void connectAttempt(Connection connection, int startIdx, int tried, CompletableFuture<Channel> cf) {
    if (closed) {
      cf.completeExceptionally(new IllegalStateException("transport closed"));
      return;
    }
    if (endpoints.isEmpty()) {
      cf.completeExceptionally(new IllegalStateException("no endpoints"));
      return;
    }
    if (tried >= endpoints.size()) {
      cf.completeExceptionally(new IllegalStateException("all endpoints failed"));
      return;
    }

    final InetSocketAddress ep = endpoints.get((startIdx + tried) % endpoints.size());
    Bootstrap b = bootstrap.clone();
    b.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
        p.addLast(new LengthFieldPrepender(4));
        p.addLast(new InboundHandler(connection));
      }
    });

    b.connect(ep).addListener((ChannelFutureListener) f -> {
      if (f.isSuccess()) {
        Channel ch = f.channel();
        connection.channel = ch;
        connection.endpoint = ep;
        connection.connecting = null;
        cf.complete(ch);
        log.info("microbucketj соединение установлено [{}]: {}", connection.index, ep);
      } else {
        connectAttempt(connection, startIdx, tried + 1, cf);
      }
    });
  }

  private CompletableFuture<Object> sendSingle(InetSocketAddress endpoint,
                                               String bucket,
                                               BucketUpsertRequest upsertReq,
                                               PayloadEncoder encoder) {
    if (closed) {
      return CompletableFuture.failedFuture(new IllegalStateException("transport closed"));
    }
    Objects.requireNonNull(endpoint, "endpoint");

    CompletableFuture<Object> out = new CompletableFuture<>();
    int reqId = ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
    Bootstrap b = bootstrap.clone();
    b.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4));
        p.addLast(new LengthFieldPrepender(4));
        p.addLast(new SimpleChannelInboundHandler<ByteBuf>() {
          private ScheduledFuture<?> timeout;

          @Override
          public void channelActive(ChannelHandlerContext ctx) {
            timeout = ctx.executor().schedule(() -> {
              if (out.completeExceptionally(new TimeoutException("request timeout"))) {
                ctx.close();
              }
            }, Math.max(1L, requestTimeout.toMillis()), TimeUnit.MILLISECONDS);
            try {
              ByteBuf payload = encoder.encode(ctx.channel(), reqId);
              ctx.writeAndFlush(payload).addListener((ChannelFutureListener) f -> {
                if (!f.isSuccess()) {
                  out.completeExceptionally(f.cause());
                  ctx.close();
                }
              });
            } catch (Throwable t) {
              out.completeExceptionally(t);
              ctx.close();
            }
          }

          @Override
          protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) {
            if (in.readableBytes() < 1 + 4) {
              return;
            }
            byte st = in.readByte();
            int responseId = in.readInt();
            if (responseId != reqId) {
              return;
            }
            Object decoded = switch (RespKind.UPSERT) {
              case ACQUIRE -> decodeAcquirePayload(st, in);
              case REPORT -> decodeReportPayload(st);
              case UPSERT -> decodeUpsertPayload(st, upsertReq, in);
              case GET -> decodeGetPayload(st, bucket, in);
            };
            if (timeout != null) {
              timeout.cancel(false);
            }
            out.complete(decoded);
            ctx.close();
          }

          @Override
          public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (timeout != null) {
              timeout.cancel(false);
            }
            out.completeExceptionally(cause);
            ctx.close();
          }

          @Override
          public void channelInactive(ChannelHandlerContext ctx) {
            if (!out.isDone()) {
              out.completeExceptionally(new IllegalStateException("channel inactive"));
            }
          }
        });
      }
    });

    b.connect(endpoint).addListener((ChannelFutureListener) f -> {
      if (!f.isSuccess()) {
        out.completeExceptionally(f.cause());
      }
    });
    return out;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;

    for (Connection c : pool) {
      Channel ch = c.channel;
      if (ch != null) {
        try {
          ch.close().syncUninterruptibly();
        } catch (Throwable ignore) {
          // ignore
        }
      }
      Exception ex = new IllegalStateException("transport closed");
      c.pending.forEach((k, v) -> v.future.completeExceptionally(ex));
      c.pending.clear();
    }

    group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
  }

  private enum RespKind {
    ACQUIRE,
    REPORT,
    UPSERT,
    GET
  }

  private static final class Pending {
    final RespKind kind;
    final String bucket;
    final BucketUpsertRequest upsertReq;
    final CompletableFuture<Object> future;

    Pending(RespKind kind, String bucket, BucketUpsertRequest upsertReq, CompletableFuture<Object> future) {
      this.kind = kind;
      this.bucket = bucket;
      this.upsertReq = upsertReq;
      this.future = future;
    }
  }

  private static final class Connection {
    final int index;
    final Object connectLock = new Object();
    final AtomicInteger requestSeq = new AtomicInteger(1);
    final ConcurrentHashMap<Integer, Pending> pending = new ConcurrentHashMap<>();

    volatile InetSocketAddress endpoint;
    volatile Channel channel;
    volatile CompletableFuture<Channel> connecting;

    Connection(int index) {
      this.index = index;
    }

    int nextRequestId() {
      for (int i = 0; i < 4; i++) {
        int id = requestSeq.getAndIncrement();
        if (id == 0) {
          continue;
        }
        if (!pending.containsKey(id)) {
          return id;
        }
      }
      return ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
    }
  }

  private static final class InboundHandler extends SimpleChannelInboundHandler<ByteBuf> {
    private final Connection connection;

    private InboundHandler(Connection connection) {
      this.connection = connection;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) {
      if (in.readableBytes() < 1 + 4) {
        return;
      }

      byte st = in.readByte();
      int reqId = in.readInt();

      Pending p = connection.pending.remove(reqId);
      if (p == null) {
        return;
      }

      try {
        Object decoded = switch (p.kind) {
          case ACQUIRE -> decodeAcquirePayload(st, in);
          case REPORT -> decodeReportPayload(st);
          case UPSERT -> decodeUpsertPayload(st, p.upsertReq, in);
          case GET -> decodeGetPayload(st, p.bucket, in);
        };
        p.future.complete(decoded);
      } catch (Throwable t) {
        p.future.completeExceptionally(t);
      }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      Exception ex = new IllegalStateException("channel inactive");
      connection.pending.forEach((k, v) -> v.future.completeExceptionally(ex));
      connection.pending.clear();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.debug("client channel exception: {}", cause.toString());
      ctx.close();
    }
  }

  private static AcquireResult decodeAcquirePayload(byte st, ByteBuf in) {
    AcquireStatus status = AcquireStatus.fromCode(st);
    long remaining = 0;
    long retryAfterMs = 0;
    if (in.readableBytes() >= 16) {
      remaining = in.readLong();
      retryAfterMs = in.readLong();
    }
    return new AcquireResult(status, remaining, retryAfterMs);
  }

  private static ReportResult decodeReportPayload(byte st) {
    return new ReportResult(st);
  }

  private static BucketUpsertResult decodeUpsertPayload(byte st, BucketUpsertRequest req, ByteBuf in) {
    String bucket = (req != null) ? req.getBucket() : "";
    if (in.readableBytes() < 1) {
      return BucketUpsertResult.unavailable(bucket);
    }
    boolean applied = in.readUnsignedByte() != 0;
    AdminSpecView spec = readAdminSpec(in);
    LeaderHint leaderHint = null;
    if (in.readableBytes() >= 1) {
      int hasHint = in.readUnsignedByte();
      if (hasHint != 0 && in.readableBytes() >= 1) {
        String leaderId = readU8(in);
        String host = readU8(in);
        int port = in.readableBytes() >= 4 ? in.readInt() : 0;
        leaderHint = new LeaderHint(leaderId, host, port);
      }
    }
    if (st == ApiProtocol.ST_REDIRECT) {
      return BucketUpsertResult.redirect(bucket, leaderHint);
    }
    return new BucketUpsertResult(bucket, st, applied,
        spec.type, spec.version,
        spec.capacity, spec.refillTokens, spec.refillPeriodMs,
        spec.stepPeriodMs, spec.notUsedResetMs,
        spec.adaptiveWindowMs,
        spec.adaptiveMinRequests, spec.adaptiveFailureRatePct,
        spec.adaptiveMinFactorPct, spec.adaptiveDecreaseStepPct,
        spec.adaptiveIncreaseStepPct, leaderHint);
  }

  private static BucketGetResult decodeGetPayload(byte st, String bucket, ByteBuf in) {
    AdminSpecView spec = readAdminSpec(in);
    return new BucketGetResult(bucket, st,
        spec.type, spec.version,
        spec.capacity, spec.refillTokens, spec.refillPeriodMs,
        spec.stepPeriodMs, spec.notUsedResetMs,
        spec.adaptiveWindowMs,
        spec.adaptiveMinRequests, spec.adaptiveFailureRatePct,
        spec.adaptiveMinFactorPct, spec.adaptiveDecreaseStepPct,
        spec.adaptiveIncreaseStepPct, spec.stepCapacities);
  }

  private static final class AdminSpecView {
    final BucketType type;
    final long version;
    final long capacity;
    final long refillTokens;
    final long refillPeriodMs;
    final long stepPeriodMs;
    final long notUsedResetMs;
    final long adaptiveWindowMs;
    final int adaptiveMinRequests;
    final int adaptiveFailureRatePct;
    final int adaptiveMinFactorPct;
    final int adaptiveDecreaseStepPct;
    final int adaptiveIncreaseStepPct;
    final long[] stepCapacities;

    private AdminSpecView(
        BucketType type,
        long version,
        long capacity,
        long refillTokens,
        long refillPeriodMs,
        long stepPeriodMs,
        long notUsedResetMs,
        long adaptiveWindowMs,
        int adaptiveMinRequests,
        int adaptiveFailureRatePct,
        int adaptiveMinFactorPct,
        int adaptiveDecreaseStepPct,
        int adaptiveIncreaseStepPct,
        long[] stepCapacities
    ) {
      this.type = type;
      this.version = version;
      this.capacity = capacity;
      this.refillTokens = refillTokens;
      this.refillPeriodMs = refillPeriodMs;
      this.stepPeriodMs = stepPeriodMs;
      this.notUsedResetMs = notUsedResetMs;
      this.adaptiveWindowMs = adaptiveWindowMs;
      this.adaptiveMinRequests = adaptiveMinRequests;
      this.adaptiveFailureRatePct = adaptiveFailureRatePct;
      this.adaptiveMinFactorPct = adaptiveMinFactorPct;
      this.adaptiveDecreaseStepPct = adaptiveDecreaseStepPct;
      this.adaptiveIncreaseStepPct = adaptiveIncreaseStepPct;
      this.stepCapacities = stepCapacities;
    }

    static AdminSpecView empty() {
      return new AdminSpecView(BucketType.TOKEN,
          0L, 0L, 0L, 0L, 0L, 0L,
          0L, 0, 0, 0, 0, 0, null);
    }
  }

  private static AdminSpecView readAdminSpec(ByteBuf in) {
    if (in == null || in.readableBytes() <= 0) {
      return AdminSpecView.empty();
    }

    if (in.readableBytes() >= 2 && in.getByte(in.readerIndex()) == ProtocolVersion.VERSIONED_MARKER) {
      in.readByte();
      byte version = in.readByte();
      ProtocolVersion.requireSupported(version);

      BucketType type = BucketType.fromWire(in.readByte());
      if (in.readableBytes() >= 1) {
        in.readByte(); // reserved
      }
      long specVersion = readLongOrZero(in);
      long capacity = readLongOrZero(in);
      long refillTokens = readLongOrZero(in);
      long refillPeriodMs = readLongOrZero(in);
      long stepPeriodMs = readLongOrZero(in);
      long notUsedResetMs = readLongOrZero(in);
      long adaptiveWindowMs = readLongOrZero(in);
      int adaptiveMinRequests = readIntOrZero(in);
      int adaptiveFailureRatePct = readIntOrZero(in);
      int adaptiveMinFactorPct = readIntOrZero(in);
      int adaptiveDecreaseStepPct = readIntOrZero(in);
      int adaptiveIncreaseStepPct = readIntOrZero(in);
      return new AdminSpecView(type, specVersion,
          capacity, refillTokens, refillPeriodMs,
          stepPeriodMs, notUsedResetMs,
          adaptiveWindowMs,
          adaptiveMinRequests, adaptiveFailureRatePct,
          adaptiveMinFactorPct, adaptiveDecreaseStepPct, adaptiveIncreaseStepPct,
          null);
    }

    long version = readLongOrZero(in);
    long capacity = readLongOrZero(in);
    long refillTokens = readLongOrZero(in);
    long refillPeriodMs = readLongOrZero(in);

    long stepPeriodMs = 0;
    long notUsedResetMs = 0;
    long[] steps = null;
    if (in.readableBytes() >= 8 + 8 + 1) {
      stepPeriodMs = in.readLong();
      notUsedResetMs = in.readLong();
      int stepCount = in.readUnsignedByte();
      if (stepCount > 0 && in.readableBytes() >= stepCount * 8) {
        steps = new long[stepCount];
        for (int i = 0; i < stepCount; i++) {
          steps[i] = in.readLong();
        }
      }
    }

    return new AdminSpecView(BucketType.TOKEN, version,
        capacity, refillTokens, refillPeriodMs,
        stepPeriodMs, notUsedResetMs,
        0L, 0, 0, 0, 0, 0,
        steps);
  }

  private static void writeVersionedSpecWithoutName(ByteBuf out, BucketUpsertRequest req) {
    out.writeByte(ProtocolVersion.VERSIONED_MARKER);
    out.writeByte(ProtocolVersion.CURRENT);
    out.writeByte(req.getType().wireCode());
    out.writeByte(0);
    out.writeLong(req.getVersion());

    long[] steps = req.getStepCapacities();
    int stepCount = Math.min(255, steps.length);
    out.writeLong(req.getCapacity());
    out.writeLong(req.getRefillTokens());
    out.writeLong(req.getRefillPeriodMs());
    out.writeLong(req.getStepPeriodMs());
    out.writeLong(req.getNotUsedResetMs());
    out.writeByte(stepCount);
    for (int i = 0; i < stepCount; i++) {
      out.writeLong(steps[i]);
    }

    if (req.isAdaptiveRate()) {
      out.writeLong(req.getAdaptiveWindowMs());
      out.writeInt(req.getAdaptiveMinRequests());
      out.writeInt(req.getAdaptiveFailureRatePct());
      out.writeInt(req.getAdaptiveMinFactorPct());
      out.writeInt(req.getAdaptiveDecreaseStepPct());
      out.writeInt(req.getAdaptiveIncreaseStepPct());
    }
  }

  private byte[] requireBucketNameBytes(String bucket) {
    Objects.requireNonNull(bucket, "bucket");
    if (bucket.isBlank()) {
      return new byte[0];
    }
    return bucketNameBytesCache.computeIfAbsent(bucket, key -> {
      byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
      if (bytes.length <= 0 || bytes.length > ApiProtocol.MAX_BUCKET_BYTES) {
        return new byte[0];
      }
      return bytes;
    });
  }

  private static void writeU8(ByteBuf out, byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      out.writeByte(0);
      return;
    }
    if (bytes.length > ApiProtocol.MAX_BUCKET_BYTES) {
      throw new IllegalArgumentException("значение слишком длинное для u8: " + bytes.length);
    }
    out.writeByte(bytes.length);
    out.writeBytes(bytes);
  }

  private static String readU8(ByteBuf in) {
    if (in == null || in.readableBytes() < 1) {
      return "";
    }
    int len = in.readUnsignedByte();
    if (len <= 0) {
      return "";
    }
    if (in.readableBytes() < len) {
      len = in.readableBytes();
    }
    byte[] bytes = new byte[len];
    in.readBytes(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static long readLongOrZero(ByteBuf in) {
    return in.readableBytes() >= 8 ? in.readLong() : 0L;
  }

  private static int readIntOrZero(ByteBuf in) {
    return in.readableBytes() >= 4 ? in.readInt() : 0;
  }

  @FunctionalInterface
  private interface PayloadEncoder {
    ByteBuf encode(Channel ch, int requestId);
  }
}
