package org.dancres.paxos.impl;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.junit.*;

/**
 * Simulate a node dying during an attempt to get consensus.
 */
public class DeadNodeTest {
    private TransportImpl _tport1;
    private DroppingTransportImpl _tport2;

    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    @Before public void init() throws Exception {
    	_node1 = new ServerDispatcher();
    	_node2 = new ServerDispatcher();
        _tport1 = new TransportImpl(new FailureDetectorImpl(5000));
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new DroppingTransportImpl(new FailureDetectorImpl(5000));
        _tport2.routeTo(_node2);
        _node2.init(_tport2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(null);
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);
        
        Proposal myProp = new Proposal("data", myBuffer.array());

        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);

        /*
         * Break node2's communications to the outside world now that we've got it in the majority.
         * The failure detector in node1 should eventually spot there are no heartbeats from node2
         * and instruct node1's leader to halt.
         */
        _tport2.setDrop(true);

        // And perform the test
        //
        myClient.send(new Envelope(myProp), _tport1.getLocalAddress());
        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse(myEv == null);

        Assert.assertTrue((myEv.getResult() == VoteOutcome.Reason.BAD_MEMBERSHIP) ||
        		(myEv.getResult() == VoteOutcome.Reason.VOTE_TIMEOUT));
    }

    static class DroppingTransportImpl extends TransportImpl {
        private boolean _drop;

        DroppingTransportImpl(MessageBasedFailureDetector anFD) throws Exception {
            super(anFD);
        }

        public void send(Packet aPacket, InetSocketAddress anAddress) {
            if (canSend())
                super.send(aPacket, anAddress);
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
