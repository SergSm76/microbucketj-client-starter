package my.app.microbucketj.client;

import my.app.microbucketj.client.model.AcquireResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public interface MicroRateLimiterClient {

  boolean tryAcquire(String bucket, long permits);

  AcquireResult tryAcquireDetailed(String bucket, long permits);

  CompletableFuture<Boolean> tryAcquireAsync(String bucket, long permits);

  CompletableFuture<AcquireResult> tryAcquireDetailedAsync(String bucket, long permits);

  boolean acquireBlocking(String bucket, long permits, Duration maxWait);

  boolean acquireBlocking(String bucket, long permits, Duration maxWait, Duration retryInterval);

  CompletableFuture<Boolean> acquireBlockingAsync(String bucket, long permits, Duration maxWait);

  CompletableFuture<Boolean> acquireBlockingAsync(String bucket, long permits, Duration maxWait, Duration retryInterval);
}
