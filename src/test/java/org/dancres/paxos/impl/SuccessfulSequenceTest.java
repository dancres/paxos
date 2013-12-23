package org.dancres.paxos.impl;

import java.nio.ByteBuffer;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.*;

public class SuccessfulSequenceTest {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        Runtime.getRuntime().runFinalizersOnExit(true);

    	_node1 = new ServerDispatcher();
    	_node2 = new ServerDispatcher();
        _tport1 = new TransportImpl(new FailureDetectorImpl(5000));
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new TransportImpl(new FailureDetectorImpl(5000));
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

        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);

        for (int i = 0; i < 5; i++) {
            ByteBuffer myBuffer = ByteBuffer.allocate(4);
            myBuffer.putInt(i);
            Proposal myProposal = new Proposal("data", myBuffer.array());

            myClient.send(new Envelope(myProposal), _tport1.getLocalAddress());

            VoteOutcome myEv = myClient.getNext(10000);

            Assert.assertFalse((myEv == null));

            Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.VALUE);
            Assert.assertTrue(myEv.getSeqNum() == i);
        }
        
        /*
         *  Let things settle before we close them off otherwise we can get a false assertion in the AL. This is
         *  because there are two nodes at play. Leader achieves success and posts this to both AL's, the first
         *  receives it and announces it to the test code which then completes and shuts things down before the
         *  second AL has finished processing the same success, by which time the log has been shut in that AL causing
         *  the assertion. 
         */
        Thread.sleep(5000);
        
        myTransport.terminate();
    }
}
