package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.dancres.paxos.AcceptorLearnerListener;
import org.dancres.paxos.Completion;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.impl.mina.io.Post;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.NodeId;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.dancres.paxos.test.utils.ClientPacketFilter;
import org.dancres.paxos.test.utils.Node;
import org.dancres.paxos.test.utils.Packet;
import org.dancres.paxos.test.utils.PacketQueue;
import org.dancres.paxos.test.utils.PacketQueueImpl;
import org.dancres.paxos.test.utils.TransportImpl;
import org.junit.*;
import org.junit.Assert.*;

public class LeaderListenerTest {
    private AddressGenerator _allocator;

    private InetSocketAddress _addr1;
    private InetSocketAddress _addr2;

    private Node _node1;
    private Node _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        _allocator = new AddressGenerator();

        _addr1 = _allocator.allocate();
        _addr2 = _allocator.allocate();

        _tport1 = new TransportImpl(_addr1);
        _tport2 = new TransportImpl(_addr2);

        _node1 = new Node(_addr1, _tport1, 5000);
        _node2 = new Node(_addr2, _tport2, 5000);

        /*
         * "Network" mappings for node1's broadcast channel
         *
         * addr1 maps to a channel that sends packets from addr1 to node1's queue
         * addr2 maps to a channel that sends packets from addr1 to node2's queue
         */
        _tport1.add(_addr1, _node1.getQueue());
        _tport1.add(_addr2, _node2.getQueue());

        /*
         * "Network" mappings for node2's broadcast channel
         *
         * addr1 maps to a channel that sends packets from addr2 to node1's queue
         * addr2 maps to a channel that sends packets from addr2 to node2's queue
         */
        _tport2.add(_addr1, _node1.getQueue());
        _tport2.add(_addr2, _node2.getQueue());

        _node1.startup();
        _node2.startup();
    }

    @Test public void post() throws Exception {
        PacketQueue myQueue = new ClientPacketFilter(new PacketQueueImpl());
        InetSocketAddress myAddr = _allocator.allocate();

        _tport1.add(myAddr, myQueue);
        _tport2.add(myAddr, myQueue);

        FailureDetectorImpl myFd = _node1.getFailureDetector();
        ListenerImpl myListener = new ListenerImpl();
        _node1.getAcceptorLearner().add(myListener);

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }

        for (int i = 0; i < 5; i++) {
            ByteBuffer myBuffer = ByteBuffer.allocate(4);
            myBuffer.putInt(i);

            _node2.getQueue().add(new Packet(NodeId.from(myAddr), new Post(myBuffer.array())));

            Packet myPacket = myQueue.getNext(10000);

            Assert.assertFalse((myPacket == null));

            PaxosMessage myMsg = myPacket.getMsg();

            System.err.println("Got message: " + myMsg.getType());

            Assert.assertTrue(myMsg.getType() == Operations.ACK);

            Assert.assertTrue(myMsg.getSeqNum() == i);
        }

        /*
         * Packets are delivered in one or more separate threads and can actually arrive (due to scheduling vagaries and multiple processors) before the
         * listener get's a signal which can mean our count can be inaccurate.  We must wait just a small amount of settling time.
         */
        Thread.sleep(2000);
        
        Assert.assertTrue("Listener count should be 5 but is: " + myListener.getCount(), myListener.testCount(5));
    }

    private class ListenerImpl implements AcceptorLearnerListener {
        private int _readyCount = 0;

        int getCount() {
            synchronized(this) {
                return _readyCount;
            }
        }
        
        boolean testCount(int aCount) {
            synchronized(this) {
                return _readyCount == aCount;
            }
        }

        public void done(Completion aCompletion) {
            synchronized(this) {
                ++_readyCount;
            }
        }
    }
}
