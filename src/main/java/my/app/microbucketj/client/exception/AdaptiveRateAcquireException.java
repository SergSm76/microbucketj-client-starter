package my.app.microbucketj.client.exception;

import my.app.microbucketj.client.model.AcquireResult;

public class AdaptiveRateAcquireException extends AdaptiveRateException {
  private final AcquireResult acquireResult;

  public AdaptiveRateAcquireException(String bucket, String message, AcquireResult acquireResult) {
    super(bucket, message);
    this.acquireResult = acquireResult;
  }

  public AcquireResult acquireResult() {
    return acquireResult;
  }
}
