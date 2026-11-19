package my.app.microbucketj.client.netty;

import my.app.microbucketj.client.MicroBucketJLockClient;
import my.app.microbucketj.client.model.LockResult;
import java.util.concurrent.CompletableFuture;

public class NettyMicroBucketJLockClient implements MicroBucketJLockClient {

  private final NettyMicroBucketJFailoverTransport transport;

  public NettyMicroBucketJLockClient(NettyMicroBucketJFailoverTransport transport) {
    this.transport = transport;
  }

  @Override public CompletableFuture<LockResult> lockAcquireAsync(String name, long lockUntilMs, String lockedBy) {
    return transport.lockAcquire(name, lockUntilMs, lockedBy);
  }
  @Override public CompletableFuture<LockResult> lockReleaseAsync(String name, String lockedBy) {
    return transport.lockRelease(name, lockedBy);
  }
  @Override public CompletableFuture<LockResult> lockUpdateAsync(String name, long lockUntilMs, String lockedBy) {
    return transport.lockUpdate(name, lockUntilMs, lockedBy);
  }
  @Override public CompletableFuture<LockResult> lockGetAsync(String name) {
    return transport.lockGet(name);
  }
  @Override public CompletableFuture<LockResult> lockDeleteAsync(String name) {
    return transport.lockDelete(name);
  }
}
