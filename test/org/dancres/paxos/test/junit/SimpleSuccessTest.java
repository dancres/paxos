package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.dancres.paxos.impl.core.Channel;
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
import org.dancres.paxos.test.utils.ChannelRegistry;
import org.dancres.paxos.test.utils.QueueChannelImpl;
import org.junit.*;
import org.junit.Assert.*;

public class SimpleSuccessTest {
    private ChannelRegistry _registry1;
    private ChannelRegistry _registry2;

    private AddressGenerator _allocator;

    private InetSocketAddress _addr1;
    private InetSocketAddress _addr2;

    private Node _node1;
    private Node _node2;

    @Before public void init() throws Exception {
        _registry1 = new ChannelRegistry();
        _registry2 = new ChannelRegistry();

        _allocator = new AddressGenerator();

        _addr1 = _allocator.allocate();
        _addr2 = _allocator.allocate();

        /*
         * BroadcastChannel imposes a FIFO ordering and we wish for the nodes
         * to be able to act independently thus we must use two separate channels.
         */
        BroadcastChannel myBroadChannel1 = new BroadcastChannel(_registry1);
        myBroadChannel1.add(_addr1);
        myBroadChannel1.add(_addr2);

        BroadcastChannel myBroadChannel2 = new BroadcastChannel(_registry2);
        myBroadChannel2.add(_addr1);
        myBroadChannel2.add(_addr2);

        _node1 = new Node(_addr1, myBroadChannel1, _registry1);
        _node2 = new Node(_addr2, myBroadChannel2, _registry2);

        /*
         * "Network" mappings for node1's broadcast channel
         *
         * addr1 maps to a channel that sends packets from addr1 to node1's queue
         * addr2 maps to a channel that sends packets from addr1 to node2's queue
         */
        _registry1.register(_addr1, new QueueChannelImpl(_addr1, _node1.getQueue()));
        _registry1.register(_addr2, new QueueChannelImpl(_addr1, _node2.getQueue()));

        /*
         * "Network" mappings for node2's broadcast channel
         *
         * addr1 maps to a channel that sends packets from addr2 to node1's queue
         * addr2 maps to a channel that sends packets from addr2 to node2's queue
         */
        _registry2.register(_addr1, new QueueChannelImpl(_addr2, _node1.getQueue()));
        _registry2.register(_addr2, new QueueChannelImpl(_addr2, _node2.getQueue()));

        _node1.startup();
        _node2.startup();
    }

    @Test public void post() throws Exception {
        PacketQueue myQueue = new PacketQueueImpl();
        InetSocketAddress myAddr = _allocator.allocate();

        _registry1.register(myAddr, new QueueChannelImpl(_addr1, myQueue));
        _registry2.register(myAddr, new QueueChannelImpl(_addr2, myQueue));

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

        Channel myChannel = new QueueChannelImpl(myAddr, _node1.getQueue());
        myChannel.write(new Post(myBuffer.array()));
        Packet myPacket = myQueue.getNext(10000);

        Assert.assertFalse((myPacket == null));

        PaxosMessage myMsg = myPacket.getMsg();

        Assert.assertTrue(myMsg.getType() == Operations.ACK);
    }
}
