package my.app.microbucketj.client.model;

import my.app.microbucketj.client.protocol.ApiProtocol;

public record LockResult(
    byte status,
    LockEntry entry
) {
  public boolean isOk() { return status == ApiProtocol.ST_OK; }
  public boolean isHeld() { return status == ApiProtocol.ST_LOCK_HELD; }
  public boolean isUnavailable() { return status == ApiProtocol.ST_UNAVAILABLE; }
  public boolean isNotFound() { return status == ApiProtocol.ST_NOT_FOUND; }
  public boolean isBadRequest() { return status == ApiProtocol.ST_BAD_REQUEST; }

  public static LockResult unavailable() { return new LockResult(ApiProtocol.ST_UNAVAILABLE, null); }
  public static LockResult badRequest() { return new LockResult(ApiProtocol.ST_BAD_REQUEST, null); }
}
