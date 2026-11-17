package my.app.microbucketj.client;

import my.app.microbucketj.client.model.AcquireResult;
import my.app.microbucketj.client.model.ReportResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("AbbreviationAsWordInName")
public interface MicroBucketJAsyncClient {

  CompletableFuture<Boolean> tryAcquireAsync(String bucket, long permits);

  CompletableFuture<AcquireResult> tryAcquireDetailedAsync(String bucket, long permits);

  CompletableFuture<Boolean> acquireBlockingAsync(String bucket, long permits, Duration maxWait);

  CompletableFuture<Boolean> acquireBlockingAsync(String bucket, long permits, Duration maxWait, Duration retryInterval);

  CompletableFuture<ReportResult> reportAsync(String bucket, boolean success);

  default CompletableFuture<Boolean> reportSuccessAsync(String bucket) {
    return reportAsync(bucket, true).thenApply(ReportResult::ok);
  }

  default CompletableFuture<Boolean> reportFailureAsync(String bucket) {
    return reportAsync(bucket, false).thenApply(ReportResult::ok);
  }
}
