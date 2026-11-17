package my.app.microbucketj.client.protocol;

public final class ApiProtocol {
  private ApiProtocol() {
  }

  public static final byte CURRENT_VERSION = ProtocolVersion.CURRENT;

  public static final byte OP_BUCKET_UPSERT = 0x02;
  public static final byte OP_BUCKET_GET = 0x03;

  public static final byte OP_ACQUIRE_HASH = 0x06;
  public static final byte OP_ACQUIRE_NAME = 0x07;
  public static final byte OP_ACQUIRE_EASY_HASH = 0x08;
  public static final byte OP_ACQUIRE_EASY_NAME = 0x09;
  public static final byte OP_ACQUIRE_1_EASY_HASH = 0x0A;
  public static final byte OP_ACQUIRE_1_EASY_NAME = 0x0B;

  public static final byte OP_REPORT_HASH = 0x0C;
  public static final byte OP_REPORT_NAME = 0x0D;

  public static final byte ST_DENIED = 0;
  public static final byte ST_ALLOWED = 1;
  public static final byte ST_UNAVAILABLE = 3;
  public static final byte ST_NOT_FOUND = 4;
  public static final byte ST_OK = 5;
  public static final byte ST_BAD_REQUEST = 6;
  public static final byte ST_COLLISION = 7;
  public static final byte ST_SHEDDED = 8;
  public static final byte ST_BREAKER_OPEN = 9;
  public static final byte ST_REDIRECT = 10;

  public static final int MAX_BUCKET_BYTES = 255;

  public static final int RESP_ACQUIRE_SIZE = 1 + 4 + 8 + 8;
  public static final int RESP_EASY_SIZE = 1 + 4;
  public static final int RESP_REPORT_SIZE = 1 + 4;

  public static final int RESP_GET_SIZE = 1 + 4 + 2 + 1 + 1 + (8 * 7) + (4 * 5);
  public static final int RESP_UPSERT_SIZE = 1 + 4 + 1 + 2 + 1 + 1 + (8 * 7) + (4 * 5);
}
