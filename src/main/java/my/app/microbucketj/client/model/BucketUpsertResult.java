package my.app.microbucketj.client.model;

import my.app.microbucketj.client.protocol.ApiProtocol;

public record BucketUpsertResult(
    String bucket,
    byte statusCode,
    boolean applied,
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
    LeaderHint leaderHint
) {

  public boolean isOk() {
    return statusCode == ApiProtocol.ST_OK;
  }

  public boolean isBadRequest() {
    return statusCode == ApiProtocol.ST_BAD_REQUEST;
  }

  public boolean isUnavailable() {
    return statusCode == ApiProtocol.ST_UNAVAILABLE;
  }

  public boolean isRedirect() {
    return statusCode == ApiProtocol.ST_REDIRECT;
  }

  public boolean isTokenBucket() {
    return type == BucketType.TOKEN;
  }

  public boolean isAdaptiveRate() {
    return type == BucketType.ADAPTIVE_RATE;
  }

  public static BucketUpsertResult unavailable(String bucket) {
    return new BucketUpsertResult(bucket, ApiProtocol.ST_UNAVAILABLE, false,
        BucketType.TOKEN,
        0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, null);
  }

  public static BucketUpsertResult redirect(String bucket, LeaderHint leaderHint) {
    return new BucketUpsertResult(bucket, ApiProtocol.ST_REDIRECT, false,
        BucketType.TOKEN,
        0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, leaderHint);
  }
}
