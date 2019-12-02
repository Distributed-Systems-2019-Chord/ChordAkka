package org.distributed.systems.chord.test;

import org.distributed.systems.chord.util.CompareUtil;

import static org.junit.Assert.assertTrue;

public class Test {

    @org.junit.Test
    public void name() {
        assertTrue(CompareUtil.between(3, true, 0, false, 6));
        assertTrue(!CompareUtil.between(0, true, 3, false, 6));
    }
}
