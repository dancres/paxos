package org.dancres.paxos.test.junit;

import java.util.HashSet;
import java.util.Set;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.junit.*;
import org.junit.Assert.*;

public class AddrRegTest {
    private AddressGenerator _registry;

    @Before public void init() throws Exception {
        _registry = new AddressGenerator();
    }

    @Test public void unique() {
        Set myAddrs = new HashSet();

        for (int i = 0; i < 5; i++) {
            myAddrs.add(_registry.allocate());
        }

        Assert.assertTrue(myAddrs.size() == 5);
    }
}
