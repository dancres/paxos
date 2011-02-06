package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Fail;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.Event;
import org.dancres.paxos.test.utils.ClientDispatcher;
import org.dancres.paxos.test.utils.ServerDispatcher;
import org.junit.*;

public class PacketDropTest {
    private TransportImpl _tport1;
    private TransportImpl _tport2;

    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    @Before public void init() throws Exception {
    	_node1 = new ServerDispatcher(5000);
    	_node2 = new ServerDispatcher(5000);
        _tport1 = new TransportImpl(_node1);
        _tport2 = new DroppingTransportImpl(_node2);
    }

    @After public void stop() throws Exception {
    	_node1.stop();
    	_node2.stop();
    }
    
    /**
     * @throws java.lang.Exception
     */
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(myClient);

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

        /*
         * Node2 should be a member as we allow it's heartbeats but no other packets to reach other nodes.
         * If there is no stable majority and we cannot circumvent packet loss we expect the leader to ultimately
         * give up.
         */
        myClient.send(new Post(myBuffer.array(), myTransport.getLocalAddress()),
        		_tport1.getLocalAddress());

        PaxosMessage myMsg = myClient.getNext(15000);

        Assert.assertFalse((myMsg == null));

        Assert.assertTrue(myMsg.getType() == Operations.FAIL);

        Fail myFail = (Fail) myMsg;

        System.err.println("Failure was: " + myFail.getReason());

        Assert.assertTrue(myFail.getReason() == Event.Reason.VOTE_TIMEOUT);
    }

    static class DroppingTransportImpl extends TransportImpl {
        private boolean _drop;

        DroppingTransportImpl(Dispatcher aDispatcher) throws Exception {
        	super(aDispatcher);
        }

        public void send(PaxosMessage aMessage, InetSocketAddress anAddress) {
            if (aMessage.getType() == Operations.HEARTBEAT)
                super.send(aMessage, anAddress);
        }
    }
}
