package org.dancres.paxos.impl;

import java.nio.ByteBuffer;

import org.dancres.paxos.*;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.test.utils.Builder;
import org.junit.*;

public class LeaderListenerTest {
    private Core _core2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        Builder myBuilder = new Builder();

        _tport1 = myBuilder.newDefaultStack();
        _tport2 = myBuilder.newDefaultTransport();
        _core2 = myBuilder.newCoreWith(_tport2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
    }
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(null);
        myClient.init(myTransport);

        FailureDetector myFd = _tport1.getFD();
        ListenerImpl myListener = new ListenerImpl();
        
        _core2.add(myListener);

        FDUtil.ensureFD(myFd);

        for (int i = 0; i < 5; i++) {
            ByteBuffer myBuffer = ByteBuffer.allocate(4);
            myBuffer.putInt(i);
            Proposal myProposal = new Proposal("data", myBuffer.array());
            
            myClient.send(new Envelope(myProposal), _tport1.getLocalAddress());

            VoteOutcome myEv = myClient.getNext(10000);

            Assert.assertFalse((myEv == null));

            Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.VALUE);
            Assert.assertTrue(myEv.getSeqNum() == i);
        }

        /*
         * Packets are delivered in one or more separate threads and can actually arrive (due to scheduling vagaries and multiple processors) before the
         * listener get's a signal which can mean our count can be inaccurate.  We must wait just a small amount of settling time.
         * Count includes a leader announcement
         */
        Thread.sleep(2000);
        
        Assert.assertTrue("Listener count should be 6 but is: " + myListener.getCount(), myListener.testCount(6));
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

        public void transition(StateEvent anEvent) {
            Assert.assertTrue(anEvent.getResult().equals(StateEvent.Reason.NEW_LEADER) ||
                    anEvent.getResult().equals(StateEvent.Reason.VALUE));

            synchronized(this) {
                ++_readyCount;
            }
        }
    }
}
