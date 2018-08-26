package com.yahoo.sherlock.utils;

import java.nio.ByteBuffer;
import java.util.Scanner;

/**
 * Utility functions specific to Numbers.
 */
public class NumberUtils {

    /**
     * Checks if a string is a valid integer in base 10.
     *
     * @param s a string to check
     * @return true if the string is a valid integer, false otherwise
     */
    public static boolean isInteger(String s) {
        if (s == null) {
            return false;
        }
        Scanner sc = new Scanner(s.trim());
        if (!sc.hasNextInt()) {
            return false;
        }
        sc.nextInt();
        return !sc.hasNext();
    }

    /**
     * Convert a 32-bit integer to its byte array.
     *
     * @param i      the integer to convert
     * @param nBytes number of bytes to use
     * @return byte array representation
     */
    public static byte[] toBytes(int i, int nBytes) {
        return ByteBuffer.allocate(nBytes).putInt(i).array();
    }

    /**
     * Convert a 32-bit integer to its 32-bit representation.
     *
     * @param i integer to convert
     * @return byte array representation
     */
    public static byte[] toBytesCompressed(int i) {
        int nIntBytes = Integer.SIZE / Byte.SIZE;
        byte[] bytes = toBytes(i, nIntBytes);
        byte[] cBytes = new byte[minBytes(i)];
        System.arraycopy(bytes, nIntBytes - cBytes.length, cBytes, 0, cBytes.length);
        return cBytes;
    }

    /**
     * Returns the minimum number of bytes
     * needed to fully store a positive integer value.
     * For zero, this method will return 1 byte.
     *
     * @param i the integer
     * @return number of bytes
     */
    public static int minBytes(int i) {
        if (i == 0) {
            return 1;
        }
        int n = 0;
        while (i != 0) {
            i >>= Byte.SIZE;
            n++;
        }
        return n;
    }

    /**
     * Decode a number of bytes into an integer value.
     * This method will pad a passed byte array if
     * there are fewer than 4 bytes.
     *
     * @param bytes an array of bytes of length no greater than 4.
     * @return integer representation
     */
    public static int decodeBytes(byte[] bytes) {
        int nIntBytes = Integer.SIZE / Byte.SIZE;
        if (bytes.length < nIntBytes) {
            byte[] iBytes = new byte[nIntBytes];
            System.arraycopy(bytes, 0, iBytes, nIntBytes - bytes.length, bytes.length);
            bytes = iBytes;
        }
        return ByteBuffer.allocate(bytes.length).put(bytes).getInt(0);
    }

    /**
     * @param str string to check
     * @return whether the string is a nonnegative integer
     */
    public static boolean isNonNegativeInt(String str) {
        return isInteger(str) && Integer.parseInt(str) >= 0;
    }

    /**
     * @param str string to check
     * @return whether the string is a nonnegative long
     */
    public static boolean isNonNegativeLong(String str) {
        return isLong(str) && Long.parseLong(str) >= 0;
    }

    /**
     * @param str string to check
     * @return whether the string is a booelan type
     */
    public static boolean isBoolean(String str) {
        if (str == null) {
            return false;
        }
        Scanner s = new Scanner(str.trim());
        if (!s.hasNextBoolean()) {
            return false;
        }
        s.nextBoolean();
        return !s.hasNext();
    }

    /**
     * @param str string to check
     * @return whether the string is a long value
     */
    public static boolean isLong(String str) {
        if (str == null) {
            return false;
        }
        Scanner s = new Scanner(str.trim());
        if (!s.hasNextLong()) {
            return false;
        }
        s.nextLong();
        return !s.hasNext();
    }

    /**
     * @param str string to check
     * @return whether the string is a double value
     */
    public static boolean isDouble(String str) {
        if (str == null) {
            return false;
        }
        Scanner s = new Scanner(str.trim());
        if (!s.hasNextDouble()) {
            return false;
        }
        s.nextDouble();
        return !s.hasNext();
    }

    /**
     * Parse a string value into an integer. If the string value
     * is null or is not an integer, returns a default value.
     * This method will never throw and returns the wrapper type.
     *
     * @param str string to parse
     * @param def default integer value
     * @return parsed integer
     */
    public static Integer parseInt(String str, Integer def) {
        if (str == null) {
            return def;
        }
        Scanner scnr = new Scanner(str);
        if (!scnr.hasNextInt()) {
            return def;
        }
        return scnr.nextInt();
    }

    /**
     * Parse a long with a default value.
     *
     * @param str string to parse
     * @param def default long value
     * @return parsed long
     */
    public static Long parseLong(String str, Long def) {
        if (str == null) {
            return def;
        }
        Scanner scnr = new Scanner(str);
        if (!scnr.hasNextLong()) {
            return def;
        }
        return scnr.nextLong();
    }

    /**
     * Parse a double with a default value.
     *
     * @param str string to parse
     * @param def default double value
     * @return parsed double
     */
    public static Double parseDouble(String str, Double def) {
        if (str == null) {
            return def;
        }
        Scanner scnr = new Scanner(str);
        if (!scnr.hasNextDouble()) {
            return def;
        }
        return scnr.nextDouble();
    }

    /**
     * Parse an integer. Returns null if the string
     * value is invalid instead of throwing.
     *
     * @param str string to parse.
     * @return parsed integer wrapper class
     */
    public static Integer parseInt(String str) {
        return parseInt(str, null);
    }

    /**
     * Parse a long.
     *
     * @param str string to parse
     * @return parsed long wrapper class
     */
    public static Long parseLong(String str) {
        return parseLong(str, null);
    }

    /**
     * Parse a double.
     *
     * @param str string to parse
     * @return parsed double wrapper class
     */
    public static Double parseDouble(String str) {
        return parseDouble(str, null);
    }

    /**
     * Parse a boolean with a default value.
     *
     * @param str string to parse
     * @param def default boolean value
     * @return parsed boolean
     */
    public static Boolean parseBoolean(String str, Boolean def) {
        if (str == null) {
            return def;
        }
        Scanner scnr = new Scanner(str);
        if (!scnr.hasNextBoolean()) {
            return def;
        }
        return scnr.nextBoolean();
    }

}
