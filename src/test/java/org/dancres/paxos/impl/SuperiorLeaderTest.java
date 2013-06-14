package org.dancres.paxos.impl;

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
import org.dancres.paxos.messages.*;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.junit.*;

public class SuperiorLeaderTest {
    private ServerDispatcher _node1;
    private ServerDispatcher _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        _node1 = new ServerDispatcher(new FailureDetectorImpl(5000));

        Core myCore = new Core(new FailureDetectorImpl(5000), new MemoryLogStorage(), null,
                CheckpointHandle.NO_CHECKPOINT, new Listener() {
            public void transition(StateEvent anEvent) {
            }
        });

        DroppingListenerImpl myDispatcher = new DroppingListenerImpl(myCore);

        _node2 = new ServerDispatcher(myCore, myDispatcher);

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
    
    @Test public void post() throws Exception {
    	ClientDispatcher myClient = new ClientDispatcher();
    	TransportImpl myTransport = new TransportImpl();
        myTransport.routeTo(myClient);
        myClient.init(myTransport);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        Proposal myProposal = new Proposal("data", myBuffer.array());
        MessageBasedFailureDetector myFd = _node1.getCore().getCommon().getPrivateFD();

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

        Assert.assertTrue(myEv.getResult() == VoteOutcome.Reason.OTHER_LEADER);
    }

    class DroppingListenerImpl implements Transport.Dispatcher {
        private Core _core;
        private Transport _tp;

        DroppingListenerImpl(Core aCore) {
            _core = aCore;
        }

        public void terminate() {
        }

        public void init(Transport aTransport) throws Exception {
            _tp = aTransport;
            _core.init(aTransport);
        }

        public boolean messageReceived(Packet aPacket) {
            PaxosMessage myMessage = aPacket.getMessage();

            switch (myMessage.getClassification()) {
                case PaxosMessage.LEADER : {
                    if (myMessage.getType() == Operations.COLLECT) {
                        Collect myCollect = (Collect) myMessage;

                        _tp.send(
                                new OldRound(myCollect.getSeqNum(), _tp.getLocalAddress(),
                                        myCollect.getRndNumber() + 1), aPacket.getSource());

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
