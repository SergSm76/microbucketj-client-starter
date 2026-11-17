package my.app.microbucketj.client.netty;

import io.netty.channel.EventLoopGroup;
import my.app.microbucketj.client.MicroBucketJAsyncClient;
import my.app.microbucketj.client.MicroBucketJBlockingClient;
import my.app.microbucketj.client.MicroBucketJClient;
import my.app.microbucketj.client.model.AcquireResult;
import my.app.microbucketj.client.model.AcquireStatus;
import my.app.microbucketj.client.model.ReportResult;
import my.app.microbucketj.client.utils.Hashing;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import my.app.microbucketj.client.protocol.ApiProtocol;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class NettyMicroBucketJClient implements MicroBucketJClient, MicroBucketJBlockingClient, MicroBucketJAsyncClient {

  private final NettyMicroBucketJFailoverTransport transport;
  private final Duration connectTimeout;
  private final Duration requestTimeout;
  private final Duration blockingRetryInterval;
  private final boolean useHashFirst;
  private final boolean failFastOnNotFound;

  private final ConcurrentHashMap<String, Boolean> forceName = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> bucketHashCache = new ConcurrentHashMap<>();

  public NettyMicroBucketJClient(
      NettyMicroBucketJFailoverTransport transport,
      Duration connectTimeout,
      Duration requestTimeout,
      Duration blockingRetryInterval,
      boolean useHashFirst,
      boolean failFastOnNotFound
  ) {
    this.transport = Objects.requireNonNull(transport, "transport");
    this.connectTimeout = Objects.requireNonNull(connectTimeout, "connectTimeout");
    this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
    this.blockingRetryInterval = Objects.requireNonNull(blockingRetryInterval, "blockingRetryInterval");
    this.useHashFirst = useHashFirst;
    this.failFastOnNotFound = failFastOnNotFound;
  }

  @Override
  public boolean tryAcquire(String bucket, long permits) {
    return tryAcquireDetailed(bucket, permits).allowed();
  }

  @Override
  public AcquireResult tryAcquireDetailed(String bucket, long permits) {
    try {
      long waitMs = Math.max(1L, requestTimeout.toMillis() + connectTimeout.toMillis());
      return tryAcquireDetailedAsync(bucket, permits).get(waitMs, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      return new AcquireResult(AcquireStatus.UNAVAILABLE, 0, 0);
    }
  }

  @Override
  public ReportResult report(String bucket, boolean success) {
    try {
      long waitMs = Math.max(1L, requestTimeout.toMillis() + connectTimeout.toMillis());
      return reportAsync(bucket, success).get(waitMs, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      return ReportResult.unavailableResult();
    }
  }

  @Override
  public boolean acquireBlocking(String bucket, long permits, Duration maxWait) {
    return acquireBlocking(bucket, permits, maxWait, blockingRetryInterval);
  }

  @Override
  public boolean acquireBlocking(String bucket, long permits, Duration maxWait, Duration retryInterval) {
    try {
      return acquireBlockingAsync(bucket, permits, maxWait, retryInterval)
          .get(maxWait.toMillis() + 50, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      return false;
    }
  }

  @Override
  public CompletableFuture<Boolean> tryAcquireAsync(String bucket, long permits) {
    return tryAcquireDetailedAsync(bucket, permits).thenApply(AcquireResult::allowed);
  }

  @Override
  public CompletableFuture<AcquireResult> tryAcquireDetailedAsync(String bucket, long permits) {
    return tryAcquireDetailedAsyncInternal(bucket, permits);
  }

  @Override
  public CompletableFuture<Boolean> acquireBlockingAsync(String bucket, long permits, Duration maxWait) {
    return acquireBlockingAsync(bucket, permits, maxWait, blockingRetryInterval);
  }

  @Override
  public CompletableFuture<Boolean> acquireBlockingAsync(String bucket, long permits, Duration maxWait, Duration retryInterval) {
    long deadlineNanos = System.nanoTime() + maxWait.toNanos();
    CompletableFuture<Boolean> out = new CompletableFuture<>();
    attemptBlocking(bucket, permits, deadlineNanos, retryInterval, out);
    return out;
  }

  @Override
  public CompletableFuture<ReportResult> reportAsync(String bucket, boolean success) {
    if (transport.isClosed()) {
      return CompletableFuture.completedFuture(ReportResult.unavailableResult());
    }
    if (bucket == null || bucket.isBlank()) {
      return CompletableFuture.completedFuture(new ReportResult(ApiProtocol.ST_NOT_FOUND));
    }

    if (useHashFirst && !forceName.getOrDefault(bucket, false)) {
      long bucketHash = bucketHashCache.computeIfAbsent(bucket, Hashing::bucketHash);
      return transport.reportByHash(bucketHash, success)
          .thenCompose(r -> {
            if (r.collision()) {
              forceName.put(bucket, true);
              return transport.reportByName(bucket, success);
            }
            return CompletableFuture.completedFuture(r);
          });
    }

    return transport.reportByName(bucket, success);
  }

  private CompletableFuture<AcquireResult> tryAcquireDetailedAsyncInternal(String bucket, long permits) {
    if (transport.isClosed()) {
      return CompletableFuture.completedFuture(new AcquireResult(AcquireStatus.UNAVAILABLE, 0, 0));
    }
    if (bucket == null || bucket.isBlank()) {
      return CompletableFuture.completedFuture(new AcquireResult(AcquireStatus.NOT_FOUND, 0, 0));
    }

    if (useHashFirst && !forceName.getOrDefault(bucket, false)) {
      long bucketHash = bucketHashCache.computeIfAbsent(bucket, Hashing::bucketHash);
      return transport.acquireByHash(bucketHash, permits)
          .thenCompose(r -> {
            if (r.status() == AcquireStatus.COLLISION) {
              forceName.put(bucket, true);
              return transport.acquireByName(bucket, permits);
            }
            return CompletableFuture.completedFuture(r);
          });
    }

    return transport.acquireByName(bucket, permits);
  }

  private void attemptBlocking(
      String bucket,
      long permits,
      long deadlineNanos,
      Duration retryInterval,
      CompletableFuture<Boolean> out
  ) {
    if (out.isDone()) {
      return;
    }
    if (transport.isClosed()) {
      out.complete(false);
      return;
    }

    CompletableFuture<AcquireResult> fut = tryAcquireDetailedAsync(bucket, permits);

    fut.whenComplete((r, e) -> {
      if (out.isDone()) {
        return;
      }

      if (e != null) {
        scheduleRetry(bucket, permits, deadlineNanos, retryInterval, out, retryInterval);
        return;
      }

      if (r.allowed()) {
        out.complete(true);
        return;
      }

      if (failFastOnNotFound && r.status() == AcquireStatus.NOT_FOUND) {
        out.complete(false);
        return;
      }

      scheduleRetry(bucket, permits, deadlineNanos, retryInterval, out,
          (r.retryAfterMs() >= 0) ? Duration.ofMillis(r.retryAfterMs()) : retryInterval);
    });
  }

  private void scheduleRetry(
      String bucket,
      long permits,
      long deadlineNanos,
      Duration configuredInterval,
      CompletableFuture<Boolean> out,
      Duration suggested
  ) {
    if (out.isDone()) {
      return;
    }

    long now = System.nanoTime();
    if (now >= deadlineNanos) {
      out.complete(false);
      return;
    }

    Duration base = (suggested != null && !suggested.isZero() && !suggested.isNegative())
        ? suggested
        : configuredInterval;

    long delayNs = base.toNanos();
    if (delayNs <= 0) {
      delayNs = 200_000L;
    }

    long jitterMax = Math.min(200_000L, Math.max(1L, delayNs / 10));
    delayNs += ThreadLocalRandom.current().nextLong(0, jitterMax + 1);

    long remainingNs = deadlineNanos - now;
    if (remainingNs <= 0) {
      out.complete(false);
      return;
    }
    delayNs = Math.min(delayNs, remainingNs);

    EventLoopGroup g = transport.group();
    g.next().schedule(
        () -> attemptBlocking(bucket, permits, deadlineNanos, configuredInterval, out),
        Math.max(1L, delayNs),
        TimeUnit.NANOSECONDS
    );
  }
}
