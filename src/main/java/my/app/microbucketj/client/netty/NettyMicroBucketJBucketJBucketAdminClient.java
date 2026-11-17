package my.app.microbucketj.client.netty;

import my.app.microbucketj.client.MicroBucketJBucketAdminClient;
import my.app.microbucketj.client.model.BucketGetResult;
import my.app.microbucketj.client.model.BucketUpsertRequest;
import my.app.microbucketj.client.model.BucketUpsertResult;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class NettyMicroBucketJBucketJBucketAdminClient implements MicroBucketJBucketAdminClient {

  private final NettyMicroBucketJFailoverTransport transport;

  public NettyMicroBucketJBucketJBucketAdminClient(NettyMicroBucketJFailoverTransport transport) {
    this.transport = Objects.requireNonNull(transport, "transport");
  }

  @Override
  public CompletableFuture<BucketUpsertResult> upsertBucketAsync(BucketUpsertRequest req) {
    return transport.upsert(req);
  }

  @Override
  public CompletableFuture<BucketGetResult> getBucketAsync(String bucket) {
    return transport.get(bucket);
  }
}
