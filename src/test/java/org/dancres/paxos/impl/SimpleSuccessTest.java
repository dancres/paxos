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

public class SimpleSuccessTest {
    private TransportImpl _tport1;
    private TransportImpl _tport2;
    
    @Before public void init() throws Exception {
        Builder myBuilder = new Builder();

        _tport1 = myBuilder.newDefaultStack();
        _tport2 = myBuilder.newDefaultStack();
    }

    @After public void stop() {
    	_tport1.terminate();
    	_tport2.terminate();
    }
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(null);
        myClient.init(myTransport);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);
        
        Proposal myProposal = new Proposal("data", myBuffer.array());
        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);

        myClient.send(new Envelope(myProposal), _tport1.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse((myEv == null));

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.VALUE);
        
        myTransport.terminate();
    }
    
    public static void main(String[] anArgs) throws Exception {
    	SimpleSuccessTest myTest = new SimpleSuccessTest();
    	myTest.init();
    	myTest.post();
    	myTest.stop();
    }
}
