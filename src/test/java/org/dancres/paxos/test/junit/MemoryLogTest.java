package org.dancres.paxos.test.junit;

import org.dancres.paxos.storage.MemoryLogStorage;
import org.junit.*;

public class MemoryLogTest {
    private MemoryLogStorage _storage;

    @Before public void init() {
        _storage = new MemoryLogStorage();
    }

    @Test public void test() throws Exception {
    	_storage.open();
    	
        byte[] myValue1 = new byte[] {1};
        byte[] myValue2 = new byte[] {2};

        long myFirst = _storage.put(myValue1, true);
        long mySecond = _storage.put(myValue2, true);

        Assert.assertEquals(0, myFirst);
        Assert.assertEquals(1, mySecond);
        
        Assert.assertArrayEquals(_storage.get(myFirst), myValue1);
        Assert.assertArrayEquals(_storage.get(mySecond), myValue2);
    }
}
