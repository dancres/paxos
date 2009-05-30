package org.dancres.paxos.test.junit;

import java.net.SocketAddress;
import org.dancres.paxos.Address;
import org.dancres.paxos.impl.util.NodeId;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.junit.*;
import org.junit.Assert.*;

public class AddressTest {
    private AddressGenerator _registry;

    @Before public void init() throws Exception {
        _registry = new AddressGenerator();
    }

    @Test public void equals() throws Exception {
        SocketAddress myAddr1 = _registry.allocate();
        SocketAddress myAddr2 = _registry.allocate();

        System.err.println(myAddr1);
        System.err.println(myAddr2);
        System.err.println(myAddr1.equals(myAddr1));

        Assert.assertTrue(NodeId.from(myAddr1).equals(NodeId.from(myAddr1)));
        Assert.assertFalse(NodeId.from(myAddr1).equals(NodeId.from(myAddr2)));
    }

    @Test public void produceString() throws Exception {
        SocketAddress myAddr1 = _registry.allocate();
        Address myAddr = NodeId.from(myAddr1);

        String myResult = myAddr.toString();

        Assert.assertTrue(myResult.length() != 0);
    }

    @Test public void checkBroadcast() throws Exception {
        SocketAddress myAddr1 = _registry.allocate();
        Address myAddr = NodeId.from(myAddr1);

        Assert.assertTrue(Address.BROADCAST.equals(Address.BROADCAST));
        Assert.assertFalse(Address.BROADCAST.equals(myAddr));
    }

    @Test public void hashWorks() throws Exception {
        SocketAddress myAddr1 = _registry.allocate();
        Address myAddr = NodeId.from(myAddr1);
        Address myOtherAddr = NodeId.from(myAddr1);

        // Two wrappers around the same address should have the same hashcode
        //
        Assert.assertTrue(myAddr.hashCode() == myOtherAddr.hashCode());
    }
}
