package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;

import org.dancres.paxos.Event;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.net.ClientDispatcher;
import org.dancres.paxos.impl.net.ServerDispatcher;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
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

    	_node1 = new ServerDispatcher(5000);
    	_node2 = new ServerDispatcher(5000);
        _tport1 = new TransportImpl();
        _tport1.add(_node1);
        _tport2 = new TransportImpl();
        _tport2.add(_node2);
    }

    @After public void stop() throws Exception {
    	_node1.stop();
    	_node2.stop();
    }
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl();
        myTransport.add(myClient);

        FailureDetector myFd = _node1.getFailureDetector();

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
            Proposal myProposal = new Proposal("data", myBuffer.array());

            myClient.send(new Envelope(myProposal, myTransport.getLocalAddress()),
            		_tport1.getLocalAddress());

            Event myEv = myClient.getNext(10000);

            Assert.assertFalse((myEv == null));

            Assert.assertTrue(myEv.getResult() == Event.Reason.DECISION);
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
        
        myClient.shutdown();
    }
}
