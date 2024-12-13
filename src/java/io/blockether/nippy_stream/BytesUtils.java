package io.blockether.nippy_stream;

public class BytesUtils {
  static public byte[] intToByteArray(int value) {
    return new byte[] {
        (byte) (value >> 24),
        (byte) (value >> 16),
        (byte) (value >> 8),
        (byte) value
    };
  }

  static public int fromByteArrayToInt(byte[] bytes) {
    return ((bytes[0] & 0xFF) << 24) |
        ((bytes[1] & 0xFF) << 16) |
        ((bytes[2] & 0xFF) << 8) |
        ((bytes[3] & 0xFF) << 0);
  }
}
