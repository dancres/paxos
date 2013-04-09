package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.dancres.paxos.*;
import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.storage.MemoryLogStorage;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Envelope;
import org.dancres.paxos.messages.Last;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
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
        _node1 = new ServerDispatcher(new FailureDetectorImpl(5000));

        Core myCore = new Core(new FailureDetectorImpl(5000), new MemoryLogStorage(), null,
                CheckpointHandle.NO_CHECKPOINT, new Listener() {
            public void transition(StateEvent anEvent) {
            }
        });

        LastListenerImpl myListener = new LastListenerImpl(myCore);

        _node2 = new ServerDispatcher(myCore, myListener);

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
            synchronized(this) {
                ++_readyCount;
            }
        }
    }
    
    @Test public void post() throws Exception {
        ListenerImpl myListener = new ListenerImpl();
        
        _node1.add(myListener);

        ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl();
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        Proposal myProposal = new Proposal("data", myBuffer.array());        
        MessageBasedFailureDetector myFd = _node1.getCommon().getPrivateFD();

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }

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
        
        Assert.assertTrue("Listener count should be 1 but is: " + myListener.getCount(), myListener.testCount(1));
    }

    class LastListenerImpl implements Transport.Dispatcher {
        private Core _core;

        LastListenerImpl(Core aCore) {
            _core = aCore;
        }

        public void init(Transport aTransport) throws Exception {
            _core.init(new LastTrapper(aTransport));
        }

        public boolean messageReceived(Packet aPacket) {
            return _core.messageReceived(aPacket);
        }

        public void terminate() {
        }

        class LastTrapper implements Transport {
            private boolean _seenLast = false;

            private Transport _tp;

            LastTrapper(Transport aTransport) {
                _tp = aTransport;
            }

            public Transport.PacketPickler getPickler() {
                return _tp.getPickler();
            }

            public void routeTo(Dispatcher aDispatcher) throws Exception {
                _tp.routeTo(aDispatcher);
            }

            public InetSocketAddress getLocalAddress() {
                return _tp.getLocalAddress();
            }

            public InetSocketAddress getBroadcastAddress() {
                return _tp.getBroadcastAddress();
            }

            /*
             * The repsonding AL will be starting from zero state and thus would normally generate a
             * "permissive" LAST that permits the leader (also at zero state) to make a new proposal
             * immediately. We want the leader to have to clear out a previous proposal prior to that. So
             * we replace the "permissive" last, introducing a valid LAST that should cause the leader to
             * deliver up the included value and then re-propose(non-Javadoc) the original value above.
             */
            public void send(PaxosMessage aMessage, InetSocketAddress anAddr) {
                if (_seenLast)
                    _tp.send(aMessage, anAddr);
                else {
                    if (aMessage.getType() == Operations.LAST) {
                        ByteBuffer myBuffer = ByteBuffer.allocate(4);
                        myBuffer.putInt(66);

                        Last myOrig = (Last) aMessage;

                        _seenLast = true;
                        _tp.send(new Last(myOrig.getSeqNum(), myOrig.getLowWatermark(), myOrig.getRndNumber() + 1,
                                new Proposal("data", myBuffer.array())), anAddr);
                    } else {
                        _tp.send(aMessage, anAddr);
                    }
                }
            }

            public void connectTo(InetSocketAddress anAddr, ConnectionHandler aHandler) {
                _tp.connectTo(anAddr, aHandler);
            }

            public void terminate() {
            }
        }
    }
}
