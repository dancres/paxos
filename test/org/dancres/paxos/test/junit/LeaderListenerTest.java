package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;
import org.dancres.paxos.Listener;
import org.dancres.paxos.Event;
import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.net.ClientDispatcher;
import org.dancres.paxos.impl.net.ServerDispatcher;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Post;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.*;

public class LeaderListenerTest {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        _node1 = new ServerDispatcher(5000);
        _node2 = new ServerDispatcher(5000);
        _tport1 = new TransportImpl(_node1);
        _tport2 = new TransportImpl(_node2);
    }

    @After public void stop() throws Exception {
    	_node1.stop();
    	_node2.stop();
    }
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(myClient);

        FailureDetector myFd = _node1.getFailureDetector();
        ListenerImpl myListener = new ListenerImpl();
        
        _node2.getAcceptorLearner().add(myListener);

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

            myClient.send(new Post(myBuffer.array(), myTransport.getLocalAddress()),
            		_tport1.getLocalAddress());

            PaxosMessage myMsg = myClient.getNext(10000);

            Assert.assertFalse((myMsg == null));

            System.err.println("Got message: " + myMsg.getType());

            Assert.assertTrue(myMsg.getType() == Operations.COMPLETE);

            Assert.assertTrue(myMsg.getSeqNum() == i);
        }

        /*
         * Packets are delivered in one or more separate threads and can actually arrive (due to scheduling vagaries and multiple processors) before the
         * listener get's a signal which can mean our count can be inaccurate.  We must wait just a small amount of settling time.
         */
        Thread.sleep(2000);
        
        Assert.assertTrue("Listener count should be 5 but is: " + myListener.getCount(), myListener.testCount(5));
    }

    private class ListenerImpl implements Listener {
        private int _readyCount = 0;

        int getCount() {
            synchronized(this) {
                return _readyCount;
            }
        }
        
        boolean testCount(int aCount) {
            synchronized(this) {
                return _readyCount == aCount;
            }
        }

        public void done(Event anEvent) {
            synchronized(this) {
                ++_readyCount;
            }
        }
    }
}
