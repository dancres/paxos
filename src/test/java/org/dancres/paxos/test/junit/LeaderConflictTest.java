package org.dancres.paxos.test.junit;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Envelope;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class LeaderConflictTest {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        Runtime.getRuntime().runFinalizersOnExit(true);

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

    private void ensureFD(MessageBasedFailureDetector anFD) throws Exception {
        int myChances = 0;

        while (!anFD.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }
    }


    @Test public void post() throws Exception {
        ClientDispatcher myClient1 = new ClientDispatcher();
        TransportImpl myTransport1 = new TransportImpl();
        myTransport1.routeTo(myClient1);
        myClient1.init(myTransport1);

        ClientDispatcher myClient2 = new ClientDispatcher();
        TransportImpl myTransport2 = new TransportImpl();
        myTransport2.routeTo(myClient2);
        myClient2.init(myTransport2);

        ensureFD(_node1.getCommon().getPrivateFD());
        ensureFD(_node2.getCommon().getPrivateFD());

        ByteBuffer myBuffer1 = ByteBuffer.allocate(4);
        ByteBuffer myBuffer2 = ByteBuffer.allocate(4);

        myBuffer1.putInt(1);
        myBuffer2.putInt(2);
        
        Proposal myProp1 = new Proposal("data", myBuffer1.array());
        Proposal myProp2 = new Proposal("data", myBuffer2.array());

        myClient1.send(new Envelope(myProp1), _tport1.getLocalAddress());

        myClient2.send(new Envelope(myProp2), _tport2.getLocalAddress());

        VoteOutcome myMsg1 = myClient1.getNext(10000);
        VoteOutcome myMsg2 = myClient2.getNext(10000);

        myTransport1.terminate();
        myTransport2.terminate();

        Assert.assertTrue(
        		(myMsg1.getResult() == VoteOutcome.Reason.OTHER_LEADER && myMsg2.getResult() == VoteOutcome.Reason.DECISION) ||
        		(myMsg1.getResult() == VoteOutcome.Reason.DECISION && myMsg2.getResult() == VoteOutcome.Reason.OTHER_LEADER));        
    }
}
