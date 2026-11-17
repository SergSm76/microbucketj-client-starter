package my.app.microbucketj.client.exception;

import my.app.microbucketj.client.model.AcquireResult;

public final class AdaptiveRateUnavailableException extends AdaptiveRateAcquireException {
  public AdaptiveRateUnavailableException(String bucket, AcquireResult acquireResult) {
    super(bucket, "Adaptive rate unavailable for bucket=" + bucket, acquireResult);
  }
}
