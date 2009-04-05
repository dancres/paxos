package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.dancres.paxos.impl.core.Channel;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Post;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.dancres.paxos.test.utils.BroadcastChannel;
import org.dancres.paxos.test.utils.ChannelRegistry;
import org.dancres.paxos.test.utils.Node;
import org.dancres.paxos.test.utils.Packet;
import org.dancres.paxos.test.utils.PacketQueue;
import org.dancres.paxos.test.utils.PacketQueueImpl;
import org.dancres.paxos.test.utils.QueueChannelImpl;
import org.junit.*;
import org.junit.Assert.*;

public class NoMajorityTest {
    private ChannelRegistry _registry1;

    private AddressGenerator _allocator;

    private InetSocketAddress _addr1;

    private Node _node1;

    @Before public void init() throws Exception {
        _registry1 = new ChannelRegistry();

        _allocator = new AddressGenerator();

        _addr1 = _allocator.allocate();

        /*
         * BroadcastChannel imposes a FIFO ordering and we wish for the nodes
         * to be able to act independently thus we must use two separate channels.
         */
        BroadcastChannel myBroadChannel1 = new BroadcastChannel(_registry1);
        myBroadChannel1.add(_addr1);

        _node1 = new Node(_addr1, myBroadChannel1, _registry1);

        /*
         * "Network" mappings for node1's broadcast channel
         *
         * addr1 maps to a channel that sends packets from addr1 to node1's queue
         */
        _registry1.register(_addr1, new QueueChannelImpl(_addr1, _node1.getQueue()));

        _node1.startup();
    }

    @Test public void post() throws Exception {
        PacketQueue myQueue = new PacketQueueImpl();
        InetSocketAddress myAddr = _allocator.allocate();

        _registry1.register(myAddr, new QueueChannelImpl(_addr1, myQueue));

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        Thread.sleep(5000);

        Channel myChannel = new QueueChannelImpl(myAddr, _node1.getQueue());
        myChannel.write(new Post(myBuffer.array()));
        Packet myPacket = myQueue.getNext(10000);

        Assert.assertFalse((myPacket == null));

        PaxosMessage myMsg = myPacket.getMsg();

        Assert.assertTrue(myMsg.getType() == Operations.FAIL);
    }
}
