package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Post;
import org.dancres.paxos.impl.faildet.FailureDetector;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.dancres.paxos.test.utils.BroadcastChannel;
import org.dancres.paxos.test.utils.Node;
import org.dancres.paxos.test.utils.Packet;
import org.dancres.paxos.test.utils.PacketQueue;
import org.dancres.paxos.test.utils.PacketQueueImpl;
import org.dancres.paxos.test.utils.QueueRegistry;
import org.junit.*;
import org.junit.Assert.*;

public class SimpleSuccessTest {
    private QueueRegistry _registry;
    private AddressGenerator _allocator;
    private Node _node1;
    private Node _node2;

    @Before public void init() throws Exception {
        _registry = new QueueRegistry();
        _allocator = new AddressGenerator();

        _node1 = new Node(_allocator.allocate(), _registry);
        _node2 = new Node(_allocator.allocate(), _registry);

        BroadcastChannel myChannel = _node1.getBroadcastChannel();
        myChannel.add(_node1.getAddr());
        myChannel.add(_node2.getAddr());

        myChannel = _node2.getBroadcastChannel();
        myChannel.add(_node1.getAddr());
        myChannel.add(_node2.getAddr());
    }

    @Test public void post() throws Exception {
        PacketQueue myQueue = new PacketQueueImpl();
        InetSocketAddress myAddr = _allocator.allocate();

        _registry.register(myAddr, myQueue);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        FailureDetector myFd = _node1.getFailureDetector();

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }

        _node1.getQueue().add(new Packet(myAddr, new Post(myBuffer.array())));
        Packet myPacket = myQueue.getNext(5000);

        Assert.assertFalse((myPacket == null));

        PaxosMessage myMsg = myPacket.getMsg();

        Assert.assertTrue(myMsg.getType() == Operations.ACK);
    }
}
