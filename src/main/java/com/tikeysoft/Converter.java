package com.tikeysoft;

public class Converter {
     public static int hexToSignedDecimal(String hex) {
        int value = Integer.parseInt(hex, 16);
        if ((value & 0x8000) != 0) {
            value -= 0x10000;
        }
        return value;
    }

     public static int hexToUnsignedDecimal(String hex) {
        return Integer.parseInt(hex, 16);
    }

     public static boolean getBitFromHex(String hex, int bitPosition) {
        int value = Integer.parseInt(hex, 16);
        return (value & (1 << (bitPosition - 1))) != 0;
    }
}
