package my.app.microbucketj.spring.autoconfig;

import my.app.microbucketj.client.MicroBucketJLockClient;
import my.app.microbucketj.client.netty.NettyMicroBucketJLockClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import my.app.microbucketj.client.MicroAdaptiveRateClient;
import my.app.microbucketj.client.MicroBucketJBucketAdminClient;
import my.app.microbucketj.client.MicroRateLimiterClient;
import my.app.microbucketj.client.netty.NettyMicroAdaptiveRateClient;
import my.app.microbucketj.client.netty.NettyMicroBucketJBucketJBucketAdminClient;
import my.app.microbucketj.client.netty.NettyMicroBucketJClient;
import my.app.microbucketj.client.netty.NettyMicroBucketJFailoverTransport;
import my.app.microbucketj.client.netty.NettyMicroBucketJTransport;
import my.app.microbucketj.client.netty.NettyMicroRateLimiterClient;
import my.app.microbucketj.spring.properties.MicroBucketJClientProperties;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(MicroBucketJClientProperties.class)
@ConditionalOnProperty(prefix = "microbucketj", name = "enabled", havingValue = "true")
public class MicroBucketJAutoConfiguration {

  @Bean(destroyMethod = "close")
  public NettyMicroBucketJFailoverTransport microBucketTransport(MicroBucketJClientProperties props) {
    List<InetSocketAddress> activeEndpoints = parseActiveEndpoints(props);
    List<InetSocketAddress> passiveEndpoints = parsePassiveEndpoints(props);

    NettyMicroBucketJTransport activeTransport = createTransport(activeEndpoints, props);
    NettyMicroBucketJTransport passiveTransport = passiveEndpoints.isEmpty() ? null : createTransport(passiveEndpoints, props);

    return new NettyMicroBucketJFailoverTransport(activeTransport, passiveTransport);
  }

  @Bean
  public NettyMicroBucketJClient microBucketClient(NettyMicroBucketJFailoverTransport transport,
                                                   MicroBucketJClientProperties props) {
    return new NettyMicroBucketJClient(
        transport,
        props.getConnectTimeout(),
        props.getRequestTimeout(),
        props.getBlockingRetryInterval(),
        props.isUseHashFirst(),
        props.isFailFastOnNotFound()
    );
  }

  @Bean
  public MicroRateLimiterClient microRateLimiterClient(NettyMicroBucketJClient core) {
    return new NettyMicroRateLimiterClient(core);
  }

  @Bean
  public MicroAdaptiveRateClient microAdaptiveRateClient(NettyMicroBucketJClient core) {
    return new NettyMicroAdaptiveRateClient(core);
  }

  @Bean
  public MicroBucketJBucketAdminClient microBucketAdminClientImpl(NettyMicroBucketJFailoverTransport transport) {
    return new NettyMicroBucketJBucketJBucketAdminClient(transport);
  }

  @Bean
  public MicroBucketJLockClient microBucketJLockClient(NettyMicroBucketJFailoverTransport transport) {
    return new NettyMicroBucketJLockClient(transport);
  }

  @Bean
  @ConditionalOnProperty(prefix = "microbucketj.client.bootstrap", name = "enabled", havingValue = "true")
  public MicroBucketJBucketBootstrapRunner microBucketBucketBootstrapRunner(MicroBucketJBucketAdminClient admin,
                                                                            MicroBucketJClientProperties props) {
    return new MicroBucketJBucketBootstrapRunner(admin, props);
  }

  private static NettyMicroBucketJTransport createTransport(List<InetSocketAddress> endpoints,
                                                            MicroBucketJClientProperties props) {
    return new NettyMicroBucketJTransport(
        endpoints,
        props.getConnectTimeout(),
        props.getRequestTimeout(),
        props.getMaxFrameSize(),
        props.getIoThreads(),
        props.getConnectionPoolSize(),
        props.isEagerConnect()
    );
  }

  private static List<InetSocketAddress> parseActiveEndpoints(MicroBucketJClientProperties props) {
    MicroBucketJClientProperties.EndpointGroup active = props.getActive();
    if (active != null && active.getEndpoints() != null && !active.getEndpoints().isEmpty()) {
      return parseEndpoints(active.getEndpoints());
    }
    return parseEndpoints(props.getEndpoints());
  }

  private static List<InetSocketAddress> parsePassiveEndpoints(MicroBucketJClientProperties props) {
    MicroBucketJClientProperties.EndpointGroup passive = props.getPassive();
    if (passive == null) {
      return List.of();
    }
    return parseEndpoints(passive.getEndpoints());
  }

  private static List<InetSocketAddress> parseEndpoints(List<String> raw) {
    List<InetSocketAddress> out = new ArrayList<>();
    if (raw == null) {
      return out;
    }

    for (String s : raw) {
      if (s == null) {
        continue;
      }
      String t = s.trim();
      if (t.isEmpty()) {
        continue;
      }

      String host;
      int port;

      int idx = t.lastIndexOf(':');
      if (idx <= 0 || idx == t.length() - 1) {
        host = t;
        port = 8085;
      } else {
        host = t.substring(0, idx);
        port = Integer.parseInt(t.substring(idx + 1));
      }

      out.add(new InetSocketAddress(host, port));
    }
    return out;
  }
}
