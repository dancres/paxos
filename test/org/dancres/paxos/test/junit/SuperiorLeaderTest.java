package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;
import org.dancres.paxos.Event;
import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.net.ClientDispatcher;
import org.dancres.paxos.impl.net.ServerDispatcher;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Fail;
import org.dancres.paxos.messages.OldRound;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.*;

public class SuperiorLeaderTest {
    private ServerDispatcher _node1;
    private OldRoundDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        _node1 = new ServerDispatcher(5000);
        _node2 = new OldRoundDispatcher(5000);

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

        FailureDetector myFd = _node1.getFailureDetector();

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }

        myClient.send(new Post(myBuffer.array(), myTransport.getLocalAddress()),
        		_tport1.getLocalAddress());

        PaxosMessage myMsg = myClient.getNext(10000);

        Assert.assertFalse((myMsg == null));

        System.err.println("Got message: " + myMsg.getType());

        Assert.assertTrue(myMsg.getType() == Operations.FAIL);

        Fail myFail = (Fail) myMsg;

        Assert.assertTrue(myFail.getReason() == Event.Reason.OTHER_LEADER);
    }

    private static class OldRoundDispatcher extends ServerDispatcher {
        public OldRoundDispatcher(long anUnresponsivenessThreshold) {
            super(anUnresponsivenessThreshold);
        }

        public void messageReceived(PaxosMessage aMessage) {
            switch (aMessage.getType()) {
                case Operations.HEARTBEAT: {
                    super.messageReceived(aMessage);

                    break;
                }

                case Operations.COLLECT: {
                	Collect myCollect = (Collect) aMessage;

                	getTransport().send(
                			new OldRound(myCollect.getSeqNum(), getTransport().getLocalAddress(),
                					myCollect.getRndNumber() + 1, getTransport().getLocalAddress()),
                					aMessage.getNodeId());

                	break;
                }

                default: {
                    getLeader().messageReceived(aMessage);
                    break;
                }
            }
        }
    }
}
