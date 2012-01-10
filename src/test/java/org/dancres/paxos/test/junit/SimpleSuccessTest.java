package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.*;

public class SimpleSuccessTest {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;
    
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
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl();
        myTransport.add(myClient);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);
        
        Proposal myProposal = new Proposal("data", myBuffer.array());
        FailureDetector myFd = _node1.getCommon().getFD();

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            System.err.println("FD says no");
            
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }

        myClient.send(new Envelope(myProposal, myTransport.getLocalAddress()),
        		_tport1.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse((myEv == null));

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.DECISION);
        
        myClient.shutdown();
    }
    
    public static void main(String[] anArgs) throws Exception {
    	SimpleSuccessTest myTest = new SimpleSuccessTest();
    	myTest.init();
    	myTest.post();
    	myTest.stop();
    }
}
