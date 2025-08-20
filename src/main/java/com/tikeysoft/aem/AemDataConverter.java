package com.tikeysoft.aem;

import static com.tikeysoft.Converter.*;

public class AemDataConverter {
    public static double convertDieTemp(String hexString) {
        return hexToSignedDecimal(hexString.substring(0, 4)) * 0.1;
    }

    public static double convertBilletTemp(String hexString) {
        return hexToSignedDecimal(hexString.substring(0, 4)) * 0.1;
    }

    public static int convertRampPressure(String hexString) {
        return (hexToSignedDecimal(hexString.substring(0, 4)) - 4) * 25;
    }

    public static double convertBillet(String hexString) {
        return (double) (hexToSignedDecimal(hexString.substring(0, 4))) /1000;
    }

    public static double convertBilletWaste(String hexString) {
        return (double) (hexToSignedDecimal(hexString.substring(0, 4)) - 1320) /1000;
    }

    public static double convertSemiProfileA(String hexString) {
        return (hexToUnsignedDecimal(hexString.substring(4, 8)) / (40.0 * 4) * 190)/1000;
    }

    public static double convertSemiProfileB(String hexString) {
        return (hexToUnsignedDecimal(hexString.substring(4, 8)) / (40.0 * 4) * 190)/1000;
    }

    public static boolean convertSignalStart(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 1);
    }

    public static boolean convertSignalEnd(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 9);
    }

    public static boolean convertBilletDetecting(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 10);
    }

    public static boolean convertBilletCutting(String hexString) {
        return getBitFromHex(hexString.substring(0, 4), 1);
    }

    public static boolean convertHeartBeat(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 2);
    }

    public static boolean convertPullerA(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 11);
    }

    public static boolean convertPullerB(String hexString) {
        return getBitFromHex(hexString.substring(4, 8), 3);
    }

    public static boolean convertCutter(String hexString) {
        return getBitFromHex(hexString.substring(0, 4), 9);
    }
}