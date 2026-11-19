package my.app.microbucketj.client.netty;

import io.netty.channel.EventLoopGroup;
import lombok.extern.slf4j.Slf4j;
import my.app.microbucketj.client.model.AcquireResult;
import my.app.microbucketj.client.model.AcquireStatus;
import my.app.microbucketj.client.model.BucketGetResult;
import my.app.microbucketj.client.model.BucketUpsertRequest;
import my.app.microbucketj.client.model.BucketUpsertResult;
import my.app.microbucketj.client.model.LockResult;
import my.app.microbucketj.client.model.ReportResult;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Slf4j
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class NettyMicroBucketJFailoverTransport implements AutoCloseable {

  private final NettyMicroBucketJTransport active;
  private final NettyMicroBucketJTransport passive;

  public NettyMicroBucketJFailoverTransport(NettyMicroBucketJTransport active, NettyMicroBucketJTransport passive) {
    this.active = active;
    this.passive = passive;
  }

  public EventLoopGroup group() {
    if (active != null) {
      return active.group();
    }
    if (passive != null) {
      return passive.group();
    }
    throw new IllegalStateException("active/passive группы эндпоинтов не сконфигурированы");
  }

  public CompletableFuture<AcquireResult> acquireByHash(long bucketHash, long permits) {
    return withFailover(
        () -> acquireOn(active, bucketHash, permits),
        () -> {
          log.warn("microbucketj client acquireByHash: активная группа эндпоинтов недоступна для bucketHash={}", bucketHash);
          return acquireOn(passive, bucketHash, permits);
        },
        r -> r == null || r.status() == AcquireStatus.UNAVAILABLE,
        () -> new AcquireResult(AcquireStatus.UNAVAILABLE, 0, 0)
    );
  }

  public CompletableFuture<AcquireResult> acquireByName(String bucket, long permits) {
    return withFailover(
        () -> acquireOn(active, bucket, permits),
        () -> {
          log.warn("microbucketj client acquireByName: активная группа эндпоинтов недоступна для bucket={}", bucket);
          return acquireOn(passive, bucket, permits);
        },
        r -> r == null || r.status() == AcquireStatus.UNAVAILABLE,
        () -> new AcquireResult(AcquireStatus.UNAVAILABLE, 0, 0)
    );
  }

  public CompletableFuture<ReportResult> reportByHash(long bucketHash, boolean success) {
    return withFailover(
        () -> reportOn(active, bucketHash, success),
        () -> reportOn(passive, bucketHash, success),
        r -> r == null || r.unavailable(),
        ReportResult::unavailableResult
    );
  }

  public CompletableFuture<ReportResult> reportByName(String bucket, boolean success) {
    return withFailover(
        () -> reportOn(active, bucket, success),
        () -> reportOn(passive, bucket, success),
        r -> r == null || r.unavailable(),
        ReportResult::unavailableResult
    );
  }

  public CompletableFuture<BucketGetResult> get(String bucket) {
    return withFailover(
        () -> getOn(active, bucket),
        () -> getOn(passive, bucket),
        r -> r == null || r.isUnavailable(),
        () -> BucketGetResult.unavailable(bucket)
    );
  }

  // --- Shedlock (Distributed Locks) Failover Methods ---
  public CompletableFuture<LockResult> lockAcquire(String name, long lockUntilMs, String lockedBy) {
    return withFailover(
        () -> lockAcquireOn(active, name, lockUntilMs, lockedBy),
        () -> {
          log.warn("microbucketj client lockAcquire: активная группа эндпоинтов недоступна для lock={}", name);
          return lockAcquireOn(passive, name, lockUntilMs, lockedBy);
        },
        r -> r == null || r.isUnavailable(),
        LockResult::unavailable
    );
  }

  public CompletableFuture<LockResult> lockRelease(String name, String lockedBy) {
    return withFailover(
        () -> lockReleaseOn(active, name, lockedBy),
        () -> {
          log.warn("microbucketj client lockRelease: активная группа эндпоинтов недоступна для lock={}", name);
          return lockReleaseOn(passive, name, lockedBy);
        },
        r -> r == null || r.isUnavailable(),
        LockResult::unavailable
    );
  }

  public CompletableFuture<LockResult> lockUpdate(String name, long lockUntilMs, String lockedBy) {
    return withFailover(
        () -> lockUpdateOn(active, name, lockUntilMs, lockedBy),
        () -> {
          log.warn("microbucketj client lockUpdate: активная группа эндпоинтов недоступна для lock={}", name);
          return lockUpdateOn(passive, name, lockUntilMs, lockedBy);
        },
        r -> r == null || r.isUnavailable(),
        LockResult::unavailable
    );
  }

  public CompletableFuture<LockResult> lockGet(String name) {
    return withFailover(
        () -> lockGetOn(active, name),
        () -> {
          log.warn("microbucketj client lockGet: активная группа эндпоинтов недоступна для lock={}", name);
          return lockGetOn(passive, name);
        },
        r -> r == null || r.isUnavailable(),
        LockResult::unavailable
    );
  }

  public CompletableFuture<LockResult> lockDelete(String name) {
    return withFailover(
        () -> lockDeleteOn(active, name),
        () -> {
          log.warn("microbucketj client lockDelete: активная группа эндпоинтов недоступна для lock={}", name);
          return lockDeleteOn(passive, name);
        },
        r -> r == null || r.isUnavailable(),
        LockResult::unavailable
    );
  }

  public CompletableFuture<BucketUpsertResult> upsert(BucketUpsertRequest req) {
    String bucket = (req != null) ? req.getBucket() : "";

    CompletableFuture<BucketUpsertResult> activeFuture = upsertOn(active, req, bucket);
    CompletableFuture<BucketUpsertResult> passiveFuture = upsertOn(passive, req, bucket);

    if (active == null) {
      return passiveFuture;
    }
    if (passive == null) {
      return activeFuture;
    }

    return activeFuture.thenCombine(passiveFuture, (activeResult, passiveResult) -> {
      if (activeResult.isUnavailable()) {
        log.warn("microbucketj admin upsert: активная группа эндпоинтов недоступна для bucket={}", bucket);
      }
      if (passiveResult.isUnavailable()) {
        log.warn("microbucketj admin upsert: пассивная группа эндпоинтов недоступна для bucket={}", bucket);
      }
      return selectUpsertResult(bucket, activeResult, passiveResult);
    });
  }

  public boolean isClosed() {
    boolean activeClosed = active == null || active.isClosed();
    boolean passiveClosed = passive == null || passive.isClosed();
    return activeClosed && passiveClosed;
  }

  @Override
  public void close() {
    closeQuietly(active);
    closeQuietly(passive);
  }

  private CompletableFuture<AcquireResult> acquireOn(NettyMicroBucketJTransport transport, long bucketHash, long permits) {
    if (transport == null) {
      return CompletableFuture.completedFuture(new AcquireResult(AcquireStatus.UNAVAILABLE, 0, 0));
    }
    return transport.acquireByHash(bucketHash, permits)
        .exceptionally(ex -> new AcquireResult(AcquireStatus.UNAVAILABLE, 0, 0));
  }

  private CompletableFuture<AcquireResult> acquireOn(NettyMicroBucketJTransport transport, String bucket, long permits) {
    if (transport == null) {
      return CompletableFuture.completedFuture(new AcquireResult(AcquireStatus.UNAVAILABLE, 0, 0));
    }
    return transport.acquireByName(bucket, permits)
        .exceptionally(ex -> new AcquireResult(AcquireStatus.UNAVAILABLE, 0, 0));
  }

  private CompletableFuture<ReportResult> reportOn(NettyMicroBucketJTransport transport, long bucketHash, boolean success) {
    if (transport == null) {
      return CompletableFuture.completedFuture(ReportResult.unavailableResult());
    }
    return transport.reportByHash(bucketHash, success)
        .exceptionally(ex -> ReportResult.unavailableResult());
  }

  private CompletableFuture<ReportResult> reportOn(NettyMicroBucketJTransport transport, String bucket, boolean success) {
    if (transport == null) {
      return CompletableFuture.completedFuture(ReportResult.unavailableResult());
    }
    return transport.reportByName(bucket, success)
        .exceptionally(ex -> ReportResult.unavailableResult());
  }

  private CompletableFuture<BucketGetResult> getOn(NettyMicroBucketJTransport transport, String bucket) {
    if (transport == null) {
      return CompletableFuture.completedFuture(BucketGetResult.unavailable(bucket));
    }
    return transport.get(bucket)
        .exceptionally(ex -> BucketGetResult.unavailable(bucket));
  }

  private CompletableFuture<BucketUpsertResult> upsertOn(NettyMicroBucketJTransport transport, BucketUpsertRequest req, String bucket) {
    if (transport == null) {
      return CompletableFuture.completedFuture(BucketUpsertResult.unavailable(bucket));
    }
    return transport.upsertFollowingRedirects(req)
        .exceptionally(ex -> BucketUpsertResult.unavailable(bucket));
  }

  private CompletableFuture<LockResult> lockAcquireOn(NettyMicroBucketJTransport transport, String name, long lockUntilMs, String lockedBy) {
    if (transport == null) {
      return CompletableFuture.completedFuture(LockResult.unavailable());
    }
    return transport.lockAcquire(name, lockUntilMs, lockedBy)
        .exceptionally(ex -> LockResult.unavailable());
  }

  private CompletableFuture<LockResult> lockReleaseOn(NettyMicroBucketJTransport transport, String name, String lockedBy) {
    if (transport == null) {
      return CompletableFuture.completedFuture(LockResult.unavailable());
    }
    return transport.lockRelease(name, lockedBy)
        .exceptionally(ex -> LockResult.unavailable());
  }

  private CompletableFuture<LockResult> lockUpdateOn(NettyMicroBucketJTransport transport, String name, long lockUntilMs, String lockedBy) {
    if (transport == null) {
      return CompletableFuture.completedFuture(LockResult.unavailable());
    }
    return transport.lockUpdate(name, lockUntilMs, lockedBy)
        .exceptionally(ex -> LockResult.unavailable());
  }

  private CompletableFuture<LockResult> lockGetOn(NettyMicroBucketJTransport transport, String name) {
    if (transport == null) {
      return CompletableFuture.completedFuture(LockResult.unavailable());
    }
    return transport.lockGet(name)
        .exceptionally(ex -> LockResult.unavailable());
  }

  private CompletableFuture<LockResult> lockDeleteOn(NettyMicroBucketJTransport transport, String name) {
    if (transport == null) {
      return CompletableFuture.completedFuture(LockResult.unavailable());
    }
    return transport.lockDelete(name)
        .exceptionally(ex -> LockResult.unavailable());
  }

  private BucketUpsertResult selectUpsertResult(String bucket, BucketUpsertResult activeResult, BucketUpsertResult passiveResult) {
    Objects.requireNonNull(activeResult, "activeResult");
    Objects.requireNonNull(passiveResult, "passiveResult");

    if (activeResult.isOk()) {
      return activeResult;
    }
    if (passiveResult.isOk()) {
      return passiveResult;
    }
    if (activeResult.isBadRequest()) {
      return activeResult;
    }
    if (passiveResult.isBadRequest()) {
      return passiveResult;
    }
    if (activeResult.isRedirect()) {
      return activeResult;
    }
    if (passiveResult.isRedirect()) {
      return passiveResult;
    }
    if (!activeResult.isUnavailable()) {
      return activeResult;
    }
    if (!passiveResult.isUnavailable()) {
      return passiveResult;
    }
    return BucketUpsertResult.unavailable(bucket);
  }

  private <T> CompletableFuture<T> withFailover(
      Supplier<CompletableFuture<T>> activeCall,
      Supplier<CompletableFuture<T>> passiveCall,
      Predicate<T> shouldFailover,
      Supplier<T> unavailableResult
  ) {
    CompletableFuture<T> first = safe(activeCall, unavailableResult);
    return first.thenCompose(result -> {
      if (!shouldFailover.test(result) || passive == null) {
        return CompletableFuture.completedFuture(result);
      }
      return safe(passiveCall, unavailableResult);
    });
  }

  private static <T> CompletableFuture<T> safe(Supplier<CompletableFuture<T>> supplier, Supplier<T> fallback) {
    try {
      CompletableFuture<T> future = supplier.get();
      if (future == null) {
        return CompletableFuture.completedFuture(fallback.get());
      }
      return future.exceptionally(ex -> fallback.get());
    } catch (Throwable t) {
      return CompletableFuture.completedFuture(fallback.get());
    }
  }

  private void closeQuietly(NettyMicroBucketJTransport transport) {
    if (transport == null) {
      return;
    }
    try {
      transport.close();
    } catch (Throwable ignore) {
      // ignore
    }
  }
}
