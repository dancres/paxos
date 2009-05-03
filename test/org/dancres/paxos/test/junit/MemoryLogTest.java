package org.dancres.paxos.test.junit;

import org.dancres.paxos.impl.util.MemoryLogStorage;
import org.junit.*;
import org.junit.Assert.*;

public class MemoryLogTest {
    private MemoryLogStorage _storage;

    @Before public void init() {
        _storage = new MemoryLogStorage();
    }

    @Test public void test() {
        byte[] myValue1 = new byte[] {1};
        byte[] myValue2 = new byte[] {2};

        _storage.put(0, myValue1);
        _storage.put(1, myValue2);

        Assert.assertArrayEquals(_storage.get(0), myValue1);
        Assert.assertArrayEquals(_storage.get(1), myValue2);
    }
}
