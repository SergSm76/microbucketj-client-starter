package my.app.microbucketj.client.model;

import my.app.microbucketj.client.protocol.ApiProtocol;

import java.util.Arrays;

public record BucketGetResult(
    String bucket,
    byte statusCode,
    BucketType type,
    long version,
    long capacity,
    long refillTokens,
    long refillPeriodMs,
    long stepPeriodMs,
    long notUsedResetMs,
    long adaptiveWindowMs,
    int adaptiveMinRequests,
    int adaptiveFailureRatePct,
    int adaptiveMinFactorPct,
    int adaptiveDecreaseStepPct,
    int adaptiveIncreaseStepPct,
    long[] stepCapacities
) {
  public BucketGetResult {
    stepCapacities = (stepCapacities == null) ? null : stepCapacities.clone();
    type = (type == null) ? BucketType.TOKEN : type;
  }

  public boolean stepped() {
    return stepCapacities != null && stepCapacities.length > 0 && stepPeriodMs > 0;
  }

  @Override
  public long[] stepCapacities() {
    return stepCapacities == null ? null : stepCapacities.clone();
  }

  public boolean isOk() {
    return statusCode == ApiProtocol.ST_OK;
  }

  public boolean isNotFound() {
    return statusCode == ApiProtocol.ST_NOT_FOUND;
  }

  public boolean isUnavailable() {
    return statusCode == ApiProtocol.ST_UNAVAILABLE;
  }

  public boolean isTokenBucket() {
    return type == BucketType.TOKEN;
  }

  public boolean isAdaptiveRate() {
    return type == BucketType.ADAPTIVE_RATE;
  }

  public static BucketGetResult unavailable(String bucket) {
    return new BucketGetResult(bucket, ApiProtocol.ST_UNAVAILABLE,
        BucketType.TOKEN,
        0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, null);
  }

  @Override
  public String toString() {
    return "bucket='"
        + "BucketGetResult{"
        + bucket + '\''
        + ", statusCode=" + statusCode
        + ", type=" + type
        + ", version=" + version
        + ", capacity=" + capacity
        + ", refillTokens=" + refillTokens
        + ", refillPeriodMs=" + refillPeriodMs
        + ", stepPeriodMs=" + stepPeriodMs
        + ", notUsedResetMs=" + notUsedResetMs
        + ", adaptiveWindowMs=" + adaptiveWindowMs
        + ", adaptiveMinRequests=" + adaptiveMinRequests
        + ", adaptiveFailureRatePct=" + adaptiveFailureRatePct
        + ", adaptiveMinFactorPct=" + adaptiveMinFactorPct
        + ", adaptiveDecreaseStepPct=" + adaptiveDecreaseStepPct
        + ", adaptiveIncreaseStepPct=" + adaptiveIncreaseStepPct
        + ", stepCapacities=" + Arrays.toString(stepCapacities) + '}';
  }
}
