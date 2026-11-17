package my.app.microbucketj.client;

import java.time.Duration;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public interface MicroBucketJBlockingClient {

  boolean acquireBlocking(String bucket, long permits, Duration maxWait);

  boolean acquireBlocking(String bucket, long permits, Duration maxWait, Duration retryInterval);
}
