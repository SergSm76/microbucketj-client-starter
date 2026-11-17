package my.app.microbucketj.client.model;

public enum BucketType {
  TOKEN((byte) 0),
  ADAPTIVE_RATE((byte) 1);

  private final byte wireCode;

  BucketType(byte wireCode) {
    this.wireCode = wireCode;
  }

  public byte wireCode() {
    return wireCode;
  }

  public boolean isTokenBucket() {
    return this == TOKEN;
  }

  public boolean isAdaptiveRate() {
    return this == ADAPTIVE_RATE;
  }

  public static BucketType fromWire(byte code) {
    return code == 1 ? ADAPTIVE_RATE : TOKEN;
  }

  public static BucketType parse(String value) {
    if (value == null || value.isBlank()) {
      return TOKEN;
    }
    String v = value.trim().toLowerCase(java.util.Locale.ROOT);
    return switch (v) {
      case "adaptive", "adaptive_rate", "adaptive-rate", "hybrid", "hybrid_rate", "hybrid-rate" -> ADAPTIVE_RATE;
      default -> TOKEN;
    };
  }
}
