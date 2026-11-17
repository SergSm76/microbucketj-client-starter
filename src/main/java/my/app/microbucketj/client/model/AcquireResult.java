package my.app.microbucketj.client.model;

import java.io.Serializable;

public record AcquireResult(
    AcquireStatus status,
    long remaining,
    long retryAfterMs
)  implements Serializable {
  public boolean allowed() {
    return status == AcquireStatus.ALLOWED;
  }

  public boolean denied() {
    return status == AcquireStatus.DENIED;
  }

  public boolean throttled() {
    return status == AcquireStatus.DENIED
        || status == AcquireStatus.SHEDDED
        || status == AcquireStatus.BREAKER_OPEN;
  }

  public boolean unavailable() {
    return status == AcquireStatus.UNAVAILABLE;
  }
}
