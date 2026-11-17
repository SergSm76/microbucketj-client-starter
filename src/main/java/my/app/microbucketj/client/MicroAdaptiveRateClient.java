package my.app.microbucketj.client;

import my.app.microbucketj.client.model.AcquireResult;
import my.app.microbucketj.client.model.ReportResult;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public interface MicroAdaptiveRateClient {

  boolean tryAcquire(String bucket);

  boolean tryAcquire(String bucket, long permits);

  AcquireResult tryAcquireDetailed(String bucket);

  AcquireResult tryAcquireDetailed(String bucket, long permits);

  CompletableFuture<Boolean> tryAcquireAsync(String bucket);

  CompletableFuture<AcquireResult> tryAcquireDetailedAsync(String bucket);

  ReportResult report(String bucket, boolean success);

  CompletableFuture<ReportResult> reportAsync(String bucket, boolean success);

  default ReportResult reportSuccess(String bucket) {
    return report(bucket, true);
  }

  default ReportResult reportFailure(String bucket) {
    return report(bucket, false);
  }

  default CompletableFuture<ReportResult> reportSuccessAsync(String bucket) {
    return reportAsync(bucket, true);
  }

  default CompletableFuture<ReportResult> reportFailureAsync(String bucket) {
    return reportAsync(bucket, false);
  }

  <T> T executeWithAdaptiveRate(String bucket, Supplier<T> action);

  void executeWithAdaptiveRate(String bucket, Runnable action);

  <T> CompletableFuture<T> executeWithAdaptiveRateAsync(
      String bucket,
      Supplier<CompletableFuture<T>> action
  );
}
