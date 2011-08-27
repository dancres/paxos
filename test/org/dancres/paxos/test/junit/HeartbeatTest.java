package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;

import org.dancres.paxos.Event;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.AcceptorLearner;
import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.net.ClientDispatcher;
import org.dancres.paxos.impl.net.ServerDispatcher;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.*;

public class HeartbeatTest {
    private TransportImpl _tport1;
    private TransportImpl _tport2;

    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    @Before public void init() throws Exception {
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
        
    private void ensureFD(FailureDetector anFD) throws Exception {
        int myChances = 0;

        while (!anFD.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }
    }

    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl();
        myTransport.add(myClient);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        Proposal myProp = new Proposal("data", myBuffer.array());
        
        ensureFD(_node1.getFailureDetector());
        ensureFD(_node2.getFailureDetector());

        myClient.send(new Envelope(myProp, myTransport.getLocalAddress()),
        		_tport2.getLocalAddress());

        Envelope myEnv = myClient.getNext(10000);

        Assert.assertFalse(myEnv == null);

        Assert.assertTrue(ServerDispatcher.getResult(myEnv) == Event.Reason.DECISION);

        // Now we have an active leader, make sure acceptor learners see heartbeats
        //
        AcceptorLearner myAl = _node2.getAcceptorLearner();

        Thread.sleep(5000 + myAl.getLeaderLeaseDuration());

        Assert.assertTrue(myAl.getHeartbeatCount() == 1);
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
