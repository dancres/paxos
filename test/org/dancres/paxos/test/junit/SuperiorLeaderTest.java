package org.dancres.paxos.test.junit;

import java.nio.ByteBuffer;

import org.dancres.paxos.impl.Core;
import org.dancres.paxos.Event;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.FailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.net.ClientDispatcher;
import org.dancres.paxos.impl.net.ServerDispatcher;
import org.dancres.paxos.messages.*;
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

        Proposal myProposal = new Proposal("data", myBuffer.array());
        FailureDetector myFd = _node1.getFailureDetector();

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }

        myClient.send(new Envelope(myProposal, myTransport.getLocalAddress()),
        		_tport1.getLocalAddress());

        Event myEv = myClient.getNext(10000);

        Assert.assertFalse((myEv == null));

        Assert.assertTrue(myEv.getResult() == Event.Reason.OTHER_LEADER);
    }

    private static class OldRoundDispatcher extends ServerDispatcher {
        public OldRoundDispatcher(long anUnresponsivenessThreshold) {
            super(anUnresponsivenessThreshold);
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

            public boolean messageReceived(PaxosMessage aMessage) {
                switch (aMessage.getClassification()) {
                    case PaxosMessage.LEADER : {
                        if (aMessage.getType() == Operations.COLLECT) {
                            Collect myCollect = (Collect) aMessage;

                            getTransport().send(
                                    new OldRound(myCollect.getSeqNum(), getTransport().getLocalAddress(),
                                            myCollect.getRndNumber() + 1, getTransport().getLocalAddress()),
                                            aMessage.getNodeId());

                            return true;
                        } else
                            return _core.messageReceived(aMessage);
                    }

                    default : {
                        return _core.messageReceived(aMessage);
                    }
                }
            }
        }
    }
}
