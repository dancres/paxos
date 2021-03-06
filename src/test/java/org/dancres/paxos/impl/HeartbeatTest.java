package org.dancres.paxos.impl;

import java.nio.ByteBuffer;

import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.test.utils.Builder;
import org.junit.*;

public class HeartbeatTest {
    private TransportImpl _tport1;
    private TransportImpl _tport2;
    private Core _core2;

    @Before public void init() throws Exception {
        Builder myBuilder = new Builder();

        _tport1 = myBuilder.newDefaultStack();
        _tport2 = myBuilder.newDefaultTransport();
        _core2 = myBuilder.newCoreWith(_tport2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }
        
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(null);
        myClient.init(myTransport);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        Proposal myProp = new Proposal("data", myBuffer.array());

        FDUtil.ensureFD(_tport1.getFD());
        FDUtil.ensureFD(_tport2.getFD());

        myClient.send(new Envelope(myProp), _tport2.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse(myEv == null);

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.VALUE);

        // Now we have an active leader, make sure acceptor learners see heartbeats
        //
        AcceptorLearner myAl = _core2.getAcceptorLearner();

        Thread.sleep(5000 + Leader.LeaseDuration.get());

        Assert.assertTrue(myAl.getStats().getHeartbeatCount() == 1);
    }
    
    public static void main(String[] anArgs) throws Exception {
		HeartbeatTest myTest = new HeartbeatTest();

		try {
    		myTest.init();
    		myTest.post();
    	} finally {
    		myTest.stop();
    	}
    }
}
