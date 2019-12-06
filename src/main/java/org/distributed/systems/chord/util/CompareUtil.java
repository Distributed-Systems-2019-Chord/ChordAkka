package org.distributed.systems.chord.util;

public class CompareUtil {

    private static boolean checkOverZero(long left, long right, long value) {
        return ((left < value && value > right) || (left > value && value < right)) && right <= left;
    }

    private static boolean checkInBetweenNotOverZero(long left, long right, long value) {
        return left < value && value < right;
    }

    public static boolean isBetweenExeclusive(long left, long right, long value) {
        return checkOverZero(left, right, value) || checkInBetweenNotOverZero(left, right, value);
    }
}
