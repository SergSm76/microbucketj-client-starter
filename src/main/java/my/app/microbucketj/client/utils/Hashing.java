package my.app.microbucketj.client.utils;

import java.nio.charset.StandardCharsets;

/**
 * Minimal hashing utilities to match server-side bucket hashing.
 */
public final class Hashing {
  private Hashing() {
  }

  /**
   * FNV-1a 64 over UTF-8 bytes, then splitmix64 finalizer.
   */
  public static long bucketHash(String bucketName) {
    byte[] b = bucketName.getBytes(StandardCharsets.UTF_8);
    return fnv1a64(b, 0, b.length);
  }

  public static long fnv1a64(byte[] data, int off, int len) {
    long h = 0xcbf29ce484222325L;
    for (int i = 0; i < len; i++) {
      h ^= (data[off + i] & 0xff);
      h *= 0x100000001b3L;
    }
    return mix64(h);
  }

  // splitmix64 finalizer (same as server)
  public static long mix64(long z) {
    z += 0x9e3779b97f4a7c15L;
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
    return z ^ (z >>> 31);
  }
}
