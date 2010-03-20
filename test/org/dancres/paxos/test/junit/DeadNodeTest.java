package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.NodeId;
import org.dancres.paxos.Event;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.dancres.paxos.test.utils.Fail;
import org.dancres.paxos.test.utils.Node;
import org.dancres.paxos.test.utils.PacketQueue;
import org.dancres.paxos.test.utils.PacketQueueImpl;
import org.dancres.paxos.test.utils.ClientPacketFilter;
import org.dancres.paxos.test.utils.TransportImpl;
import org.junit.*;

/**
 * Simulate a node dying during an attempt to get consensus.
 */
public class DeadNodeTest {
	private static final byte[] HANDBACK = new byte[]{1, 2, 3, 4};
	
    private AddressGenerator _allocator;

    private InetSocketAddress _addr1;
    private InetSocketAddress _addr2;

    private TransportImpl _tport1;
    private DroppingTransportImpl _tport2;

    private Node _node1;
    private Node _node2;

    @Before public void init() throws Exception {
        _allocator = new AddressGenerator();

        _addr1 = _allocator.allocate();
        _addr2 = _allocator.allocate();

        _tport1 = new TransportImpl(NodeId.from(_addr1));
        _tport2 = new DroppingTransportImpl(NodeId.from(_addr2));

        _node1 = new Node(_tport1, 5000);
        // _node1.getLeader().setLeaderCheck(false);
        _node2 = new Node(_tport2, 5000);

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

    @After public void stop() throws Exception {
    	_node1.stop();
    	_node2.stop();
    }
    
    @Test public void post() throws Exception {
        PacketQueue myQueue = new ClientPacketFilter(new PacketQueueImpl());
        InetSocketAddress myAddr = _allocator.allocate();

        _tport1.add(myAddr, myQueue);
        _tport2.add(myAddr, myQueue);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        FailureDetectorImpl myFd = _node1.getFailureDetector();

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }

        /*
         * Break node2's communications to the outside world now that we've got it in the majority.
         * The failure detector in node1 should eventually spot there are no heartbeats from node2
         * and instruct node1's leader to halt.
         */
        _tport2.setDrop(true);

        // And perform the test
        //
        _node1.getQueue().add(new Post(myBuffer.array(), HANDBACK, 
        		NodeId.from(myAddr).asLong()));
        PaxosMessage myMsg = myQueue.getNext(10000);

        Assert.assertFalse((myMsg == null));

        Assert.assertTrue(myMsg.getType() == Operations.FAIL);

        Fail myFail = (Fail) myMsg;

        Assert.assertTrue(myFail.getReason() == Event.Reason.BAD_MEMBERSHIP);
    }

    static class DroppingTransportImpl extends TransportImpl {
        private boolean _drop;

        DroppingTransportImpl(NodeId aNodeId) {
            super(aNodeId);
        }

        public void send(PaxosMessage aMessage, NodeId anAddress) {
            if (canSend())
                super.send(aMessage, anAddress);
        }

        private boolean canSend() {
            synchronized(this) {
                return ! _drop;
            }
        }

        void setDrop(boolean aDropStatus) {
            synchronized(this) {
                _drop = aDropStatus;
            }
        }
    }
}
