package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;

import org.dancres.paxos.impl.net.ClientDispatcher;
import org.dancres.paxos.impl.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.junit.*;

public class NoMajorityTest {
    private ServerDispatcher _node1;

    private TransportImpl _tport1;

    @Before public void init() throws Exception {
        _node1 = new ServerDispatcher(5000);
        _tport1 = new TransportImpl();
        _tport1.add(_node1);
    }

    @After public void stop() throws Exception {
    	_node1.stop();
    }
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl();
        myTransport.add(myClient);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);
        Proposal myProposal = new Proposal("data", myBuffer.array());

        Thread.sleep(5000);

        myClient.send(new Envelope(myProposal, myTransport.getLocalAddress()),
        		_tport1.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse((myEv == null));

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.BAD_MEMBERSHIP);
    }
}
