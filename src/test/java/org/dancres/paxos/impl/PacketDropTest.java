package org.dancres.paxos.impl;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.junit.*;

public class PacketDropTest {
    private TransportImpl _tport1;
    private TransportImpl _tport2;

    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    @Before public void init() throws Exception {
    	_node1 = new ServerDispatcher();
    	_node2 = new ServerDispatcher();
        _tport1 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new DroppingTransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _tport2.routeTo(_node2);
        _node2.init(_tport2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }
    
    /**
     * @throws java.lang.Exception
     */
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(null);
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

    	ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        Proposal myProposal = new Proposal("data", myBuffer.array());
        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);

        /*
         * Node2 should be a member as we allow it's heartbeats but no other packets to reach other nodes.
         * If there is no stable majority and we cannot circumvent packet loss we expect the leader to ultimately
         * give up.
         */
        myClient.send(new Envelope(myProposal), _tport1.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(15000);

        Assert.assertFalse((myEv == null));

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.VOTE_TIMEOUT);
    }

    static class DroppingTransportImpl extends TransportImpl {
        private boolean _drop;

        DroppingTransportImpl(MessageBasedFailureDetector anFD) throws Exception {
        	super(anFD);
        }

        public void send(Packet aPacket, InetSocketAddress anAddress) {
            if (aPacket.getMessage().getType() == PaxosMessage.Types.HEARTBEAT)
                super.send(aPacket, anAddress);
        }
    }
}
