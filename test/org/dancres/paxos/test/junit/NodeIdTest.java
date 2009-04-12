package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import org.dancres.paxos.impl.util.NodeId;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.junit.*;
import org.junit.Assert.*;

public class NodeIdTest {
    @Test public void simple() throws Exception {
        AddressGenerator myGen = new AddressGenerator();
        InetSocketAddress myAddr = myGen.allocate();

        long myId = NodeId.from(myAddr);

        System.err.println(Long.toHexString(myId));
        System.err.println(NodeId.toAddress(myId));

        Assert.assertEquals(myAddr, NodeId.toAddress(myId));
    }
}
