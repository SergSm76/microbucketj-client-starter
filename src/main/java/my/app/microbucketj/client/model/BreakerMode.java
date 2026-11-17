package my.app.microbucketj.client.model;

public enum BreakerMode {
  PARTIAL((byte) 0),
  FULL((byte) 1);

  private final byte wireCode;

  BreakerMode(byte wireCode) {
    this.wireCode = wireCode;
  }

  public byte wireCode() {
    return wireCode;
  }

  public static BreakerMode fromWire(byte code) {
    return code == 1 ? FULL : PARTIAL;
  }

  public static BreakerMode parse(String value) {
    if (value == null || value.isBlank()) {
      return PARTIAL;
    }
    String v = value.trim().toLowerCase(java.util.Locale.ROOT);
    return switch (v) {
      case "full", "open" -> FULL;
      default -> PARTIAL;
    };
  }
}
