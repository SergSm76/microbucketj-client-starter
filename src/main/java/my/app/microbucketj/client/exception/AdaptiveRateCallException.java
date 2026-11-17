package my.app.microbucketj.client.exception;

public final class AdaptiveRateCallException extends AdaptiveRateException {
  public AdaptiveRateCallException(String bucket, Throwable cause) {
    super(bucket, "Adaptive rate protected call failed for bucket=" + bucket, cause);
  }
}
