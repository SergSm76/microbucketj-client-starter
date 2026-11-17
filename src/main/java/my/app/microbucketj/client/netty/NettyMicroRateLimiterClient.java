package my.app.microbucketj.client.netty;

import my.app.microbucketj.client.MicroRateLimiterClient;
import my.app.microbucketj.client.model.AcquireResult;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class NettyMicroRateLimiterClient implements MicroRateLimiterClient {

  private final NettyMicroBucketJClient core;

  public NettyMicroRateLimiterClient(NettyMicroBucketJClient core) {
    this.core = Objects.requireNonNull(core, "core");
  }

  @Override
  public boolean tryAcquire(String bucket, long permits) {
    return core.tryAcquire(bucket, permits);
  }

  @Override
  public AcquireResult tryAcquireDetailed(String bucket, long permits) {
    return core.tryAcquireDetailed(bucket, permits);
  }

  @Override
  public CompletableFuture<Boolean> tryAcquireAsync(String bucket, long permits) {
    return core.tryAcquireAsync(bucket, permits);
  }

  @Override
  public CompletableFuture<AcquireResult> tryAcquireDetailedAsync(String bucket, long permits) {
    return core.tryAcquireDetailedAsync(bucket, permits);
  }

  @Override
  public boolean acquireBlocking(String bucket, long permits, Duration maxWait) {
    return core.acquireBlocking(bucket, permits, maxWait);
  }

  @Override
  public boolean acquireBlocking(String bucket, long permits, Duration maxWait, Duration retryInterval) {
    return core.acquireBlocking(bucket, permits, maxWait, retryInterval);
  }

  @Override
  public CompletableFuture<Boolean> acquireBlockingAsync(String bucket, long permits, Duration maxWait) {
    return core.acquireBlockingAsync(bucket, permits, maxWait);
  }

  @Override
  public CompletableFuture<Boolean> acquireBlockingAsync(String bucket, long permits, Duration maxWait, Duration retryInterval) {
    return core.acquireBlockingAsync(bucket, permits, maxWait, retryInterval);
  }
}
