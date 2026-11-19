package my.app.microbucketj.client;

import my.app.microbucketj.client.model.LockResult;
import java.util.concurrent.CompletableFuture;

public interface MicroBucketJLockClient {

  CompletableFuture<LockResult> lockAcquireAsync(String name, long lockUntilMs, String lockedBy);
  CompletableFuture<LockResult> lockReleaseAsync(String name, String lockedBy);
  CompletableFuture<LockResult> lockUpdateAsync(String name, long lockUntilMs, String lockedBy);
  CompletableFuture<LockResult> lockGetAsync(String name);
  CompletableFuture<LockResult> lockDeleteAsync(String name);

  default LockResult lockAcquire(String name, long lockUntilMs, String lockedBy) {
    return lockAcquireAsync(name, lockUntilMs, lockedBy).join();
  }
  default LockResult lockRelease(String name, String lockedBy) {
    return lockReleaseAsync(name, lockedBy).join();
  }
  default LockResult lockUpdate(String name, long lockUntilMs, String lockedBy) {
    return lockUpdateAsync(name, lockUntilMs, lockedBy).join();
  }
  default LockResult lockGet(String name) {
    return lockGetAsync(name).join();
  }
  default LockResult lockDelete(String name) {
    return lockDeleteAsync(name).join();
  }
}
