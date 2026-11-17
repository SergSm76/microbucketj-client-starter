package my.app.microbucketj.client.exception;

import my.app.microbucketj.client.model.AcquireResult;

public final class AdaptiveRateNotFoundException extends AdaptiveRateAcquireException {
  public AdaptiveRateNotFoundException(String bucket, AcquireResult acquireResult) {
    super(bucket, "Adaptive rate bucket not found: " + bucket, acquireResult);
  }
}
