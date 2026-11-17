package my.app.microbucketj.spring.autoconfig;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import my.app.microbucketj.client.MicroBucketJBucketAdminClient;
import my.app.microbucketj.client.model.BucketType;
import my.app.microbucketj.client.model.BucketUpsertRequest;
import my.app.microbucketj.client.model.BucketUpsertResult;
import my.app.microbucketj.spring.properties.MicroBucketJClientProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class MicroBucketJBucketBootstrapRunner implements ApplicationRunner {

  private final MicroBucketJBucketAdminClient admin;
  private final MicroBucketJClientProperties props;

  public MicroBucketJBucketBootstrapRunner(MicroBucketJBucketAdminClient admin, MicroBucketJClientProperties props) {
    this.admin = Objects.requireNonNull(admin, "admin");
    this.props = Objects.requireNonNull(props, "props");
  }

  @Override
  public void run(ApplicationArguments args) {
    MicroBucketJClientProperties.Bootstrap b = props.getBootstrap();
    if (b == null || !b.isEnabled()) {
      return;
    }

    List<BucketUpsertRequest> reqs = new ArrayList<>();

    if (b.getBuckets() != null) {
      for (MicroBucketJClientProperties.Bucket it : b.getBuckets()) {
        if (it == null) {
          continue;
        }
        String name = it.getName();
        if (name == null || name.isBlank()) {
          continue;
        }

        long[] steps = null;
        if (it.getStepCapacities() != null && !it.getStepCapacities().isEmpty()) {
          steps = new long[it.getStepCapacities().size()];
          for (int i = 0; i < steps.length; i++) {
            steps[i] = it.getStepCapacities().get(i);
          }
        }

        BucketType type = BucketType.parse(it.getType());
        if (type.isAdaptiveRate()) {
          reqs.add(BucketUpsertRequest.adaptiveRate(
              name,
              it.getVersion(),
              it.getCapacity(),
              it.getRefillTokens(),
              it.getRefillPeriodMs(),
              it.getStepPeriodMs(),
              it.getNotUsedResetMs(),
              steps,
              it.getAdaptiveWindowMs(),
              it.getAdaptiveMinRequests(),
              it.getAdaptiveFailureRatePct(),
              it.getAdaptiveMinFactorPct(),
              it.getAdaptiveDecreaseStepPct(),
              it.getAdaptiveIncreaseStepPct()
          ));
        } else {
          reqs.add(new BucketUpsertRequest(
              name,
              it.getVersion(),
              it.getCapacity(),
              it.getRefillTokens(),
              it.getRefillPeriodMs(),
              it.getStepPeriodMs(),
              it.getNotUsedResetMs(),
              steps
          ));
        }
      }
    }

    if (reqs.isEmpty()) {
      log.warn("microbucketj bootstrap включен, но ни одного бакета не настроено");
      return;
    }

    Duration timeout = (b.getTimeout() != null) ? b.getTimeout() : Duration.ofSeconds(10);
    log.info("microbucketj bootstrap: обновление {} бакета(ов) (best-effort), timeout={}", reqs.size(), timeout);

    for (BucketUpsertRequest desired : reqs) {
      BucketUpsertResult r = admin.upsertBucket(desired, timeout);
      if (r.isOk()) {
        log.info("microbucketj bootstrap: bucket={} обновление успешно, applied={} type={} ver={} cap={} refill={} periodMs={}",
            r.bucket(), r.applied(), r.type(), r.version(), r.capacity(), r.refillTokens(), r.refillPeriodMs());
      } else {
        log.warn("microbucketj bootstrap: bucket={} обновление неуспешно, statusCode={} applied={} (best-effort)",
            r.bucket(), r.statusCode(), r.applied());
      }
    }
  }
}
