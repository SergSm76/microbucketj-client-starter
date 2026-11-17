package my.app.microbucketj.client;

import my.app.microbucketj.client.model.BucketGetResult;
import my.app.microbucketj.client.model.BucketUpsertRequest;
import my.app.microbucketj.client.model.BucketUpsertResult;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public interface MicroBucketJBucketAdminClient {

  CompletableFuture<BucketUpsertResult> upsertBucketAsync(BucketUpsertRequest req);

  CompletableFuture<BucketGetResult> getBucketAsync(String bucket);

  default BucketUpsertResult upsertBucket(BucketUpsertRequest req, Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    try {
      long ms = Math.max(1L, timeout.toMillis());
      return upsertBucketAsync(req).get(ms, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      return BucketUpsertResult.unavailable(req != null ? req.getBucket() : "");
    }
  }

  default BucketGetResult getBucket(String bucket, Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    try {
      long ms = Math.max(1L, timeout.toMillis());
      return getBucketAsync(bucket).get(ms, TimeUnit.MILLISECONDS);
    } catch (Throwable t) {
      return BucketGetResult.unavailable(bucket != null ? bucket : "");
    }
  }
}
