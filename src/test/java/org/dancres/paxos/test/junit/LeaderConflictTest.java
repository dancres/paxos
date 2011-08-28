package org.dancres.paxos.test.junit;

import org.dancres.paxos.Event;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.net.ClientDispatcher;
import org.dancres.paxos.impl.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
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
        ClientDispatcher myClient1 = new ClientDispatcher();
        TransportImpl myTransport1 = new TransportImpl();
        myTransport1.add(myClient1);

        ClientDispatcher myClient2 = new ClientDispatcher();
        TransportImpl myTransport2 = new TransportImpl();
        myTransport2.add(myClient2);

        ensureFD(_node1.getFailureDetector());
        ensureFD(_node2.getFailureDetector());

        ByteBuffer myBuffer1 = ByteBuffer.allocate(4);
        ByteBuffer myBuffer2 = ByteBuffer.allocate(4);

        myBuffer1.putInt(1);
        myBuffer2.putInt(2);
        
        Proposal myProp1 = new Proposal("data", myBuffer1.array());
        Proposal myProp2 = new Proposal("data", myBuffer2.array());

        myClient1.send(new Envelope(myProp1, myTransport1.getLocalAddress()),
                _tport1.getLocalAddress());

        myClient2.send(new Envelope(myProp2, myTransport2.getLocalAddress()),
                _tport2.getLocalAddress());

        Event myMsg1 = myClient1.getNext(10000);
        Event myMsg2 = myClient2.getNext(10000);

        myClient1.shutdown();
        myClient2.shutdown();

        Assert.assertTrue(
        		(myMsg1.getResult() == Event.Reason.OTHER_LEADER && myMsg2.getResult() == Event.Reason.DECISION) ||
        		(myMsg1.getResult() == Event.Reason.DECISION && myMsg2.getResult() == Event.Reason.OTHER_LEADER));        
    }
}
