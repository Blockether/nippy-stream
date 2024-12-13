package io.blockether.nippy_stream;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * Used internally to provide chunk size metadata
 * 4 bytes are reserved for the size of the chunk
 * Used only for the nippy fast thaw..
 *
 * !! DANGER !!
 * Should not be used with the REAL Output Streams!
 *
 * !! DANGER !!
 * Mark has to be confirmed before the stream is used
 */
public class ByteArrayOutputStreamWithReservedSizeMark extends ByteArrayOutputStream {

    public ByteArrayOutputStreamWithReservedSizeMark() {
        super();

        this.count = Integer.BYTES;
    }

    public ByteArrayOutputStreamWithReservedSizeMark(int size) {
        super(size);

        this.count = Integer.BYTES;
    }

    synchronized public void syncMarkAfterWrite() {
        System.arraycopy(BytesUtils.intToByteArray(count - Integer.BYTES), 0, buf, 0, Integer.BYTES);
    }
}
