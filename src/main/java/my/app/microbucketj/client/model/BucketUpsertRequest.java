package my.app.microbucketj.client.model;

import java.util.Objects;

public final class BucketUpsertRequest {
  private final String bucket;
  private final long version;
  private final BucketType type;

  private final long capacity;
  private final long refillTokens;
  private final long refillPeriodMs;
  private final long stepPeriodMs;
  private final long notUsedResetMs;
  private final long[] stepCapacities;

  private final long adaptiveWindowMs;
  private final int adaptiveMinRequests;
  private final int adaptiveFailureRatePct;
  private final int adaptiveMinFactorPct;
  private final int adaptiveDecreaseStepPct;
  private final int adaptiveIncreaseStepPct;

  public BucketUpsertRequest(
      String bucket,
      long version,
      long capacity,
      long refillTokens,
      long refillPeriodMs,
      long stepPeriodMs,
      long notUsedResetMs,
      long[] stepCapacities
  ) {
    this(bucket, version, BucketType.TOKEN,
        capacity, refillTokens, refillPeriodMs,
        stepPeriodMs, notUsedResetMs, stepCapacities,
        0L, 0, 0, 0, 0, 0);
  }

  public BucketUpsertRequest(
      String bucket,
      long version,
      BucketType type,
      long capacity,
      long refillTokens,
      long refillPeriodMs,
      long stepPeriodMs,
      long notUsedResetMs,
      long[] stepCapacities,
      long adaptiveWindowMs,
      int adaptiveMinRequests,
      int adaptiveFailureRatePct,
      int adaptiveMinFactorPct,
      int adaptiveDecreaseStepPct,
      int adaptiveIncreaseStepPct
  ) {
    Objects.requireNonNull(bucket, "bucket");
    if (bucket.isBlank()) {
      throw new IllegalArgumentException("bucket должен быть непустым");
    }
    if (version <= 0) {
      throw new IllegalArgumentException("version должен быть > 0");
    }
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity должен быть > 0");
    }
    if (refillTokens <= 0) {
      throw new IllegalArgumentException("refillTokens должен быть > 0");
    }
    if (refillPeriodMs <= 0) {
      throw new IllegalArgumentException("refillPeriodMs должен быть > 0");
    }
    if (notUsedResetMs < 0) {
      throw new IllegalArgumentException("notUsedResetMs должен быть >= 0");
    }

    this.bucket = bucket;
    this.version = version;
    this.type = (type == null) ? BucketType.TOKEN : type;
    this.capacity = capacity;
    this.refillTokens = refillTokens;
    this.refillPeriodMs = refillPeriodMs;
    this.stepPeriodMs = stepPeriodMs;
    this.notUsedResetMs = notUsedResetMs;

    long[] normalizedSteps = normalizeSteps(stepCapacities, capacity);
    if (normalizedSteps.length > 0 && stepPeriodMs <= 0) {
      throw new IllegalArgumentException("stepPeriodMs должен быть > 0 когда stepCapacities заданы");
    }
    this.stepCapacities = normalizedSteps;

    if (this.type.isAdaptiveRate()) {
      if (adaptiveWindowMs <= 0) {
        throw new IllegalArgumentException("adaptiveWindowMs должен быть > 0");
      }
      if (adaptiveMinRequests <= 0) {
        throw new IllegalArgumentException("adaptiveMinRequests должен быть > 0");
      }
      if (adaptiveFailureRatePct <= 0 || adaptiveFailureRatePct > 100) {
        throw new IllegalArgumentException("adaptiveFailureRatePct должен быть in 1..100");
      }
      if (adaptiveMinFactorPct <= 0 || adaptiveMinFactorPct > 100) {
        throw new IllegalArgumentException("adaptiveMinFactorPct должен быть in 1..100");
      }
      if (adaptiveDecreaseStepPct <= 0 || adaptiveDecreaseStepPct > 100) {
        throw new IllegalArgumentException("adaptiveDecreaseStepPct должен быть in 1..100");
      }
      if (adaptiveIncreaseStepPct <= 0 || adaptiveIncreaseStepPct > 100) {
        throw new IllegalArgumentException("adaptiveIncreaseStepPct должен быть in 1..100");
      }
      this.adaptiveWindowMs = adaptiveWindowMs;
      this.adaptiveMinRequests = adaptiveMinRequests;
      this.adaptiveFailureRatePct = adaptiveFailureRatePct;
      this.adaptiveMinFactorPct = adaptiveMinFactorPct;
      this.adaptiveDecreaseStepPct = adaptiveDecreaseStepPct;
      this.adaptiveIncreaseStepPct = adaptiveIncreaseStepPct;
    } else {
      this.adaptiveWindowMs = 0L;
      this.adaptiveMinRequests = 0;
      this.adaptiveFailureRatePct = 0;
      this.adaptiveMinFactorPct = 0;
      this.adaptiveDecreaseStepPct = 0;
      this.adaptiveIncreaseStepPct = 0;
    }
  }

  public static BucketUpsertRequest simple(String bucket, long version, long capacity, long refillTokens, long refillPeriodMs) {
    return new BucketUpsertRequest(bucket, version, capacity, refillTokens, refillPeriodMs, 0, 0, null);
  }

  public static BucketUpsertRequest warmup(
      String bucket,
      long version,
      long capacity,
      long refillTokens,
      long refillPeriodMs,
      long stepPeriodMs,
      long notUsedResetMs,
      long[] stepCapacities
  ) {
    return new BucketUpsertRequest(bucket, version, capacity, refillTokens, refillPeriodMs, stepPeriodMs, notUsedResetMs, stepCapacities);
  }

  public static BucketUpsertRequest adaptiveRate(
      String bucket,
      long version,
      long capacity,
      long refillTokens,
      long refillPeriodMs,
      long stepPeriodMs,
      long notUsedResetMs,
      long[] stepCapacities,
      long adaptiveWindowMs,
      int adaptiveMinRequests,
      int adaptiveFailureRatePct,
      int adaptiveMinFactorPct,
      int adaptiveDecreaseStepPct,
      int adaptiveIncreaseStepPct
  ) {
    return new BucketUpsertRequest(bucket, version, BucketType.ADAPTIVE_RATE,
        capacity, refillTokens, refillPeriodMs,
        stepPeriodMs, notUsedResetMs, stepCapacities,
        adaptiveWindowMs, adaptiveMinRequests, adaptiveFailureRatePct,
        adaptiveMinFactorPct, adaptiveDecreaseStepPct, adaptiveIncreaseStepPct);
  }

  public String getBucket() {
    return bucket;
  }

  public long getVersion() {
    return version;
  }

  public BucketType getType() {
    return type;
  }

  public long getCapacity() {
    return capacity;
  }

  public long getRefillTokens() {
    return refillTokens;
  }

  public long getRefillPeriodMs() {
    return refillPeriodMs;
  }

  public long getStepPeriodMs() {
    return stepPeriodMs;
  }

  public long getNotUsedResetMs() {
    return notUsedResetMs;
  }

  public long[] getStepCapacities() {
    return stepCapacities.clone();
  }

  public long getAdaptiveWindowMs() {
    return adaptiveWindowMs;
  }

  public int getAdaptiveMinRequests() {
    return adaptiveMinRequests;
  }

  public int getAdaptiveFailureRatePct() {
    return adaptiveFailureRatePct;
  }

  public int getAdaptiveMinFactorPct() {
    return adaptiveMinFactorPct;
  }

  public int getAdaptiveDecreaseStepPct() {
    return adaptiveDecreaseStepPct;
  }

  public int getAdaptiveIncreaseStepPct() {
    return adaptiveIncreaseStepPct;
  }

  public boolean isTokenBucket() {
    return type == BucketType.TOKEN;
  }

  public boolean isAdaptiveRate() {
    return type == BucketType.ADAPTIVE_RATE;
  }

  private static long[] normalizeSteps(long[] stepCapacities, long capacity) {
    if (stepCapacities == null || stepCapacities.length == 0) {
      return new long[0];
    }
    long[] copy = stepCapacities.clone();
    for (int i = 0; i < copy.length; i++) {
      if (copy[i] <= 0) {
        throw new IllegalArgumentException("stepCapacities[" + i + "] должен быть > 0");
      }
    }
    if (copy[copy.length - 1] != capacity) {
      long[] extended = new long[copy.length + 1];
      System.arraycopy(copy, 0, extended, 0, copy.length);
      extended[copy.length] = capacity;
      return extended;
    }
    return copy;
  }
}
