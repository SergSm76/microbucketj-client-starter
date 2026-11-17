package my.app.microbucketj.client.model;

import my.app.microbucketj.client.protocol.ApiProtocol;

public enum AcquireStatus {
  DENIED(ApiProtocol.ST_DENIED),
  ALLOWED(ApiProtocol.ST_ALLOWED),
  UNAVAILABLE(ApiProtocol.ST_UNAVAILABLE),
  NOT_FOUND(ApiProtocol.ST_NOT_FOUND),
  COLLISION(ApiProtocol.ST_COLLISION),
  SHEDDED(ApiProtocol.ST_SHEDDED),
  BREAKER_OPEN(ApiProtocol.ST_BREAKER_OPEN);

  private final byte code;

  AcquireStatus(byte code) {
    this.code = code;
  }

  public byte code() {
    return code;
  }

  public static AcquireStatus fromCode(byte code) {
    return switch (code) {
      case ApiProtocol.ST_ALLOWED -> ALLOWED;
      case ApiProtocol.ST_DENIED -> DENIED;
      case ApiProtocol.ST_NOT_FOUND -> NOT_FOUND;
      case ApiProtocol.ST_COLLISION -> COLLISION;
      case ApiProtocol.ST_SHEDDED -> SHEDDED;
      case ApiProtocol.ST_BREAKER_OPEN -> BREAKER_OPEN;
      default -> UNAVAILABLE;
    };
  }
}
