package my.app.microbucketj.spring.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@ConfigurationProperties(prefix = "microbucketj.client")
@Data
public class MicroBucketJClientProperties {

  private List<String> endpoints = new ArrayList<>(List.of("localhost:8085"));
  private EndpointGroup active = new EndpointGroup();
  private EndpointGroup passive = new EndpointGroup();
  private Duration connectTimeout = Duration.ofSeconds(1);
  private Duration requestTimeout = Duration.ofMillis(250);
  private Duration blockingRetryInterval = Duration.ofMillis(25);
  private boolean useHashFirst = true;
  private boolean failFastOnNotFound = true;
  private int maxFrameSize = 1024;
  private int ioThreads = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
  private int connectionPoolSize = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors()));
  private boolean eagerConnect = true;
  private Bootstrap bootstrap = new Bootstrap();

  @Data
  public static class EndpointGroup {
    private List<String> endpoints = new ArrayList<>();
  }

  @Data
  public static class Bootstrap {
    private boolean enabled = false;
    private Duration timeout = Duration.ofSeconds(10);
    private List<Bucket> buckets = new ArrayList<>();
  }

  @Data
  public static class Bucket {
    private String name;
    private long version = 1;

    /// token|adaptive_rate
    private String type = "token";

    private long capacity;
    private long refillTokens;
    private long refillPeriodMs;
    private List<Long> stepCapacities = new ArrayList<>();
    private long stepPeriodMs;
    private long notUsedResetMs;

    private long adaptiveWindowMs;
    private int adaptiveMinRequests;
    private int adaptiveFailureRatePct;
    private int adaptiveMinFactorPct;
    private int adaptiveDecreaseStepPct;
    private int adaptiveIncreaseStepPct;
  }
}
