package my.app.microbucketj.client.protocol;

public final class ProtocolVersion {
  private ProtocolVersion() {
  }

  public static final byte VERSIONED_MARKER = 0x7F;
  public static final byte V1 = 1;
  public static final byte CURRENT = V1;
  public static final byte MIN_SUPPORTED = V1;

  public static boolean isSupported(byte version) {
    return version >= MIN_SUPPORTED && version <= CURRENT;
  }

  public static void requireSupported(byte version) {
    if (!isSupported(version)) {
      throw new IllegalArgumentException("неподдерживаемая версия протокола: " + version);
    }
  }
}
