package org.dancres.paxos.impl;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.storage.MemoryLogStorage;
import org.dancres.paxos.test.junit.FDUtil;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.messages.Last;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LastHandlingTest {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    public static void main(String[] anArgs) throws Exception {
    	LastHandlingTest myTest = new LastHandlingTest();
    	myTest.init();
    	myTest.post();
    	myTest.stop();
    }
    
    @Before public void init() throws Exception {
        _node1 = new ServerDispatcher();
        _node2 = new ServerDispatcher();

        _tport1 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _tport1.routeTo(_node1);
        _node1.init(_tport1);

        _tport2 = new TransportImpl(new FailureDetectorImpl(5000, FailureDetectorImpl.OPEN_PIN));
        _tport2.filterTx(new LastDropper());
        _tport2.routeTo(_node2);
        _node2.init(_tport2);
    }

    @After public void stop() throws Exception {
    	_tport1.terminate();
    	_tport2.terminate();
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
    
    @Test public void post() throws Exception {
        ListenerImpl myListener = new ListenerImpl();
        
        _node1.add(myListener);

        ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl(null);
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        Proposal myProposal = new Proposal("data", myBuffer.array());        
        FailureDetector myFd = _tport1.getFD();

        FDUtil.ensureFD(myFd);

        myClient.send(new Envelope(myProposal), _tport1.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse((myEv == null));

        /*
         * Leader will have another value so will tell us the original proposal has been replaced by the last.
         * It will then push through what was the last, leading to an update on the listener. Client code would
         * have to re-submit the request for it's value but we're not doing that in this case...
         */
        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.OTHER_VALUE);

        Thread.sleep(5000);

        // Listener should see announce of a leader and a value
        //
        Assert.assertTrue("Listener count should be 2 but is: " + myListener.getCount(), myListener.testCount(2));
    }

    class LastDropper implements Transport.Filter {
        private boolean _seenLast = false;

        @Override
        public Packet filter(Transport aTransport, Packet aPacket) {
            if (_seenLast)
                return aPacket;
            else {
                if (aPacket.getMessage().getType() == PaxosMessage.Types.LAST) {
                    ByteBuffer myBuffer = ByteBuffer.allocate(4);
                    myBuffer.putInt(66);

                    Last myOrig = (Last) aPacket.getMessage();

                    _seenLast = true;
                    return aTransport.getPickler().newPacket(new Last(myOrig.getSeqNum(), myOrig.getLowWatermark(),
                            myOrig.getRndNumber() + 1,
                            new Proposal("data", myBuffer.array())));
                } else {
                    return aPacket;
                }
            }
        }
    }
}
