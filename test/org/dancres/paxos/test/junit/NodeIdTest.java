package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import org.dancres.paxos.NodeId;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.junit.*;
import org.junit.Assert.*;

public class NodeIdTest {
    @Test public void simple() throws Exception {
        AddressGenerator myGen = new AddressGenerator();
        InetSocketAddress myAddr = myGen.allocate();

        NodeId myId = NodeId.from(myAddr);

        Assert.assertEquals(myAddr, NodeId.toAddress(myId));
    }
}
