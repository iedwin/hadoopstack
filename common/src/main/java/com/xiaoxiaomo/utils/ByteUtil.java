package com.xiaoxiaomo.utils;

/**
 *
 * Created by xiaoxiaomo on 2017/6/1.
 */
public class ByteUtil {


    public static byte[] combinByte(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    public static byte[] combinByte(byte[] a, byte[] b, byte[] c) {
        byte[] result = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        System.arraycopy(c, 0, result, a.length + b.length, c.length);
        return result;
    }

    public static byte[] combinByte(byte[] a, byte[] b, byte[] c, byte[] d) {
        byte[] result = new byte[a.length + b.length + c.length + d.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        System.arraycopy(c, 0, result, a.length + b.length, c.length);
        System.arraycopy(d, 0, result, a.length + b.length + c.length, d.length);
        return result;
    }

    public static byte[] combinByte(byte[] a, byte[] b, byte[] c, byte[] d,byte[] e) {
        byte[] result = new byte[a.length + b.length + c.length + d.length + e.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        System.arraycopy(c, 0, result, a.length + b.length, c.length);
        System.arraycopy(d, 0, result, a.length + b.length + c.length, d.length);
        System.arraycopy(e, 0, result, a.length + b.length + c.length + d.length, e.length);
        return result;
    }
    
    public static void main(String[] args) {
        byte[] he = {50,48,56,51,54,52,56,50,48,48,48,48,48,48,48,48};
        String hehe = new String(he);
        System.out.println(hehe);
    }
}
