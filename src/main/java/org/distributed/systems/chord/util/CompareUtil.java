package org.distributed.systems.chord.util;

public class CompareUtil {

    public static boolean between(long beginKey, boolean includingLowerBound, long endKey, boolean includingUpperBound, long id) {
        if (beginKey > endKey) { // we go over the zero point
            if (includingLowerBound && includingUpperBound) {
                return !(id < beginKey && id > endKey);
            } else if (includingLowerBound) {
                return !(id < beginKey && id >= endKey);
            } else if (includingUpperBound) {
                return !(id <= beginKey && id > endKey);
            } else {
                return !(id <= beginKey && id >= endKey);
            }
        } else if (endKey > beginKey) {
            if (includingLowerBound && includingUpperBound) {
                return (id >= beginKey && id <= endKey);
            } else if (includingLowerBound) {
                return (id >= beginKey && id < endKey);
            } else if (includingUpperBound) {
                return (id > beginKey && id <= endKey);
            } else {
                return (id > beginKey && id < endKey);
            }
        } else { // (endKey == beginKey)
            return true; // There is just one node
        }
    }
}
