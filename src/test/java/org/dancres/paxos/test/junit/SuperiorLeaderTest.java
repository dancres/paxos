package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.Core;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.MessageBasedFailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.test.net.ClientDispatcher;
import org.dancres.paxos.test.net.ServerDispatcher;
import org.dancres.paxos.messages.*;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.*;

public class SuperiorLeaderTest {
    private ServerDispatcher _node1;
    private OldRoundDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        _node1 = new ServerDispatcher(new FailureDetectorImpl(5000));
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

        Proposal myProposal = new Proposal("data", myBuffer.array());
        MessageBasedFailureDetector myFd = _node1.getCommon().getPrivateFD();

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }

        myClient.send(new Envelope(myProposal, myTransport.getLocalAddress()),
        		_tport1.getLocalAddress());

        VoteOutcome myEv = myClient.getNext(10000);

        Assert.assertFalse((myEv == null));

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.OTHER_LEADER);
    }

    private static class OldRoundDispatcher extends ServerDispatcher {
        public OldRoundDispatcher(long anUnresponsivenessThreshold) {
            super(new FailureDetectorImpl(anUnresponsivenessThreshold));
        }

        /*
         * Override original setTransport which sets Core as a direct listener
         */
        public void setTransport(Transport aTransport) throws Exception {
            _tp = aTransport;
            _tp.add(new DroppingListenerImpl(_core));
        }

        class DroppingListenerImpl implements Transport.Dispatcher {
            private Core _core;

            DroppingListenerImpl(Core aCore) {
                _core = aCore;
            }

            public void setTransport(Transport aTransport) throws Exception {
                _core.setTransport(aTransport);
            }

            public boolean messageReceived(Packet aPacket) {
            	PaxosMessage myMessage = aPacket.getMessage();
            	
                switch (myMessage.getClassification()) {
                    case PaxosMessage.LEADER : {
                        if (myMessage.getType() == Operations.COLLECT) {
                            Collect myCollect = (Collect) myMessage;

                            getTransport().send(
                                    new OldRound(myCollect.getSeqNum(), getTransport().getLocalAddress(),
                                            myCollect.getRndNumber() + 1, getTransport().getLocalAddress()),
                                            aPacket.getSource());

                            return true;
                        } else
                            return _core.messageReceived(aPacket);
                    }

                    default : {
                        return _core.messageReceived(aPacket);
                    }
                }
            }
        }
    }
}
