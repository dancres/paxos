package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.AcceptorLearner;
import org.dancres.paxos.impl.Common;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Collect;
import org.junit.*;

public class IgnoreCollectsTest {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
    	_node1 = new ServerDispatcher(new FailureDetectorImpl(5000));
    	_node2 = new ServerDispatcher(new FailureDetectorImpl(5000));
        _tport1 = new TransportImpl();
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new TransportImpl();
        _tport2.routeTo(_node2);
        _node2.init(_tport2);
    }
    
    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl();
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        MessageBasedFailureDetector myFd = _node1.getCommon().getPrivateFD();

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }


        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);
        Proposal myProp = new Proposal("data", myBuffer.array());
        
        myClient.send(new Envelope(myProp), _tport1.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse((myEv == null));

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.DECISION);

        // Now we have an active leader, make sure acceptor learners ignore contenders
        AcceptorLearner myAl = _node2.getAcceptorLearner();
        Common myCommon = _node2.getCommon();

        Collect myCollect = new Collect(myCommon.getRecoveryTrigger().getLowWatermark().getSeqNum() + 1,
        		myCommon.getLeaderRndNum());

        myClient.send(myCollect, _tport2.getLocalAddress());

        // Must wait for message to make it's way to acceptor learners
        Thread.sleep(5000);

        Assert.assertTrue(myAl.getIgnoredCollectsCount() == 1);
    }
}
