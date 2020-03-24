package org.dancres.paxos.impl;

import java.nio.ByteBuffer;

import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.test.utils.Builder;
import org.junit.*;

public class IgnoreCollectsTest {
    private Core _core2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

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

        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);
        Proposal myProp = new Proposal("data", myBuffer.array());
        
        myClient.send(new Envelope(myProp), _tport1.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse((myEv == null));

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.VALUE);

        // Now we have an active leader, make sure acceptor learners ignore contenders
        AcceptorLearner myAl = _core2.getAcceptorLearner();
        Common myCommon = _core2.getCommon();

        Collect myCollect = new Collect(myAl.getLowWatermark().getSeqNum() + 1,
        		myAl.getLeaderRndNum() + 1);

        myClient.send(myCollect, _tport2.getLocalAddress());

        // Must wait for message to make it's way to acceptor learners
        Thread.sleep(5000);

        Assert.assertTrue(myAl.getStats().getIgnoredCollectsCount() == 1);
    }
}
