package my.app.microbucketj.client.exception;

public class AdaptiveRateException extends RuntimeException {
  private final String bucket;

  public AdaptiveRateException(String bucket, String message) {
    super(message);
    this.bucket = bucket;
  }

  public AdaptiveRateException(String bucket, String message, Throwable cause) {
    super(message, cause);
    this.bucket = bucket;
  }

  public String bucket() {
    return bucket;
  }
}
