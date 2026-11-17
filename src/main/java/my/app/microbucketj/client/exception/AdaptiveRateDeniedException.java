package my.app.microbucketj.client.exception;

import my.app.microbucketj.client.model.AcquireResult;

public final class AdaptiveRateDeniedException extends AdaptiveRateAcquireException {
  public AdaptiveRateDeniedException(String bucket, AcquireResult acquireResult) {
    super(bucket, "Adaptive rate denied request for bucket=" + bucket, acquireResult);
  }
}
