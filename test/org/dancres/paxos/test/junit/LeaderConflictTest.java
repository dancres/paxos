package org.dancres.paxos.test.junit;

import org.dancres.paxos.Event;
import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Fail;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.dancres.paxos.test.utils.ClientDispatcher;
import org.dancres.paxos.test.utils.ServerDispatcher;
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
        _tport1 = new TransportImpl(_node1);
        _tport2 = new TransportImpl(_node2);
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
        TransportImpl myTransport1 = new TransportImpl(myClient1);

        ClientDispatcher myClient2 = new ClientDispatcher();
        TransportImpl myTransport2 = new TransportImpl(myClient2);

        ensureFD(_node1.getFailureDetector());
        ensureFD(_node2.getFailureDetector());

        ByteBuffer myBuffer1 = ByteBuffer.allocate(4);
        ByteBuffer myBuffer2 = ByteBuffer.allocate(4);

        myBuffer1.putInt(1);
        myBuffer2.putInt(2);

        myClient1.send(new Post(myBuffer1.array(), myTransport1.getLocalAddress()),
                _tport1.getLocalAddress());

        myClient2.send(new Post(myBuffer2.array(), myTransport2.getLocalAddress()),
                _tport2.getLocalAddress());

        PaxosMessage myMsg1 = myClient1.getNext(10000);
        PaxosMessage myMsg2 = myClient2.getNext(10000);

        myClient1.shutdown();
        myClient2.shutdown();

        Assert.assertTrue((myMsg1.getType() == Operations.FAIL && myMsg2.getType() == Operations.COMPLETE) ||
            (myMsg1.getType() == Operations.COMPLETE && myMsg2.getType() == Operations.FAIL));

        if (myMsg1.getType() == Operations.FAIL) {
            Fail myFail = (Fail) myMsg1;

            assert(myFail.getReason() == Event.Reason.OTHER_LEADER);
        } else {
            Fail myFail = (Fail) myMsg2;

            assert(myFail.getReason() == Event.Reason.OTHER_LEADER);
        }
    }
}
