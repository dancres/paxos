package org.dancres.paxos.test.junit;

import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Set;
import org.dancres.paxos.test.utils.AddressRegistry;
import org.junit.*;
import org.junit.Assert.*;

public class AddrRegTest {
    private AddressRegistry _registry;

    @Before public void init() throws Exception {
        _registry = new AddressRegistry();
    }

    @Test public void unique() {
        Set myAddrs = new HashSet();

        for (int i = 0; i < 5; i++) {
            myAddrs.add(_registry.allocate(new Integer(5)));
        }

        Assert.assertTrue(myAddrs.size() == 5);
    }
}
