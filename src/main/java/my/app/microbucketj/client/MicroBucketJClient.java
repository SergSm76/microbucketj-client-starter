package my.app.microbucketj.client;

import my.app.microbucketj.client.model.AcquireResult;
import my.app.microbucketj.client.model.ReportResult;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public interface MicroBucketJClient {

  boolean tryAcquire(String bucket, long permits);

  AcquireResult tryAcquireDetailed(String bucket, long permits);

  ReportResult report(String bucket, boolean success);

  default boolean reportSuccess(String bucket) {
    return report(bucket, true).ok();
  }

  default boolean reportFailure(String bucket) {
    return report(bucket, false).ok();
  }
}
