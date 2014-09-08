package fundb.utils;

import java.util.Formatter;

public class BytesUtil {

    public static final byte[] toArray(long v) {
        final byte[] arr = new byte[8];

        arr[0] = (byte) (0xff & (v >> 56));
        arr[1] = (byte) (0xff & (v >> 48));
        arr[2] = (byte) (0xff & (v >> 40));
        arr[3] = (byte) (0xff & (v >> 32));
        arr[4] = (byte) (0xff & (v >> 24));
        arr[5] = (byte) (0xff & (v >> 16));
        arr[6] = (byte) (0xff & (v >> 8));
        arr[7] = (byte) (0xff & v);

        return arr;
    }

    public static final long toLong(byte[] arr) {
        return (((long) (arr[0] & 0xff) << 56) |
                ((long) (arr[1] & 0xff) << 48) |
                ((long) (arr[2] & 0xff) << 40) |
                ((long) (arr[3] & 0xff) << 32) |
                ((long) (arr[4] & 0xff) << 24) |
                ((long) (arr[5] & 0xff) << 16) |
                ((long) (arr[6] & 0xff) << 8) |
                ((long) (arr[7] & 0xff)));
    }

    public static Appendable hexEncode(byte buf[], Appendable sb) {
        final Formatter formatter = new Formatter(sb);
        for (int i = 0; i < buf.length; i++) {
            formatter.format("%02x", buf[i]);
        }
        return sb;
    }

    public static String hexEncode(String s) {
        return hexEncode(s.getBytes(), new StringBuilder()).toString();
    }

}
