package org.dancres.paxos.test.junit;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import org.dancres.paxos.impl.core.Reasons;
import org.dancres.paxos.impl.core.Transport;
import org.dancres.paxos.impl.core.messages.Begin;
import org.dancres.paxos.impl.core.messages.Fail;
import org.dancres.paxos.impl.core.messages.OldRound;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.io.mina.Post;
import org.dancres.paxos.impl.core.messages.ProposerPacket;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.impl.util.AddressImpl;
import org.dancres.paxos.test.utils.AddressGenerator;
import org.dancres.paxos.test.utils.ClientPacketFilter;
import org.dancres.paxos.test.utils.Node;
import org.dancres.paxos.test.utils.Packet;
import org.dancres.paxos.test.utils.PacketQueue;
import org.dancres.paxos.test.utils.PacketQueueImpl;
import org.dancres.paxos.test.utils.TransportImpl;
import org.junit.*;
import org.junit.Assert.*;

public class SuperiorLeaderAtBeginTest {
    private AddressGenerator _allocator;

    private InetSocketAddress _addr1;
    private InetSocketAddress _addr2;

    private Node _node1;
    private OldRoundNode _node2;

    private TransportImpl _tport1;
    private TransportImpl _tport2;

    @Before public void init() throws Exception {
        _allocator = new AddressGenerator();

        _addr1 = _allocator.allocate();
        _addr2 = _allocator.allocate();

        _tport1 = new TransportImpl(_addr1);
        _tport2 = new TransportImpl(_addr2);

        _node1 = new Node(_addr1, _tport1, 5000);
        _node2 = new OldRoundNode(_addr2, _tport2, 5000);

        /*
         * "Network" mappings for node1's broadcast channel
         *
         * addr1 maps to a channel that sends packets from addr1 to node1's queue
         * addr2 maps to a channel that sends packets from addr1 to node2's queue
         */
        _tport1.add(_addr1, _node1.getQueue());
        _tport1.add(_addr2, _node2.getQueue());

        /*
         * "Network" mappings for node2's broadcast channel
         *
         * addr1 maps to a channel that sends packets from addr2 to node1's queue
         * addr2 maps to a channel that sends packets from addr2 to node2's queue
         */
        _tport2.add(_addr1, _node1.getQueue());
        _tport2.add(_addr2, _node2.getQueue());

        _node1.startup();
        _node2.startup();
    }

    @Test public void post() throws Exception {
        PacketQueue myQueue = new ClientPacketFilter(new PacketQueueImpl());
        InetSocketAddress myAddr = _allocator.allocate();

        _tport1.add(myAddr, myQueue);
        _tport2.add(myAddr, myQueue);

        ByteBuffer myBuffer = ByteBuffer.allocate(4);
        myBuffer.putInt(55);

        FailureDetectorImpl myFd = _node1.getFailureDetector();

        int myChances = 0;

        while (!myFd.couldComplete()) {
            ++myChances;
            if (myChances == 4)
                Assert.assertTrue("Membership not achieved", false);

            Thread.sleep(5000);
        }

        _node1.getQueue().add(new Packet(new AddressImpl(myAddr), new Post(myBuffer.array())));

        Packet myPacket = myQueue.getNext(10000);

        Assert.assertFalse((myPacket == null));

        PaxosMessage myMsg = myPacket.getMsg();

        System.err.println("Got message: " + myMsg.getType());

        Assert.assertTrue(myMsg.getType() == Operations.FAIL);

        Fail myFail = (Fail) myMsg;

        Assert.assertTrue(myFail.getReason() == Reasons.OTHER_LEADER);
    }

    private static class OldRoundNode extends Node {
        public OldRoundNode(InetSocketAddress anAddr, Transport aTransport, long anUnresponsivenessThreshold) {
            super(anAddr, aTransport, anUnresponsivenessThreshold);
        }

        public void deliver(Packet aPacket) throws Exception {
            PaxosMessage myMessage = aPacket.getMsg();

            switch (myMessage.getType()) {
                case Heartbeat.TYPE: {
                    getFailureDetector().processMessage(myMessage, aPacket.getSender());

                    break;
                }

                case Operations.PROPOSER_REQ: {
                    ProposerPacket myPropPkt = (ProposerPacket) myMessage;
                    PaxosMessage myIn = myPropPkt.getOperation();

                    if (myIn.getType() == Operations.BEGIN) {
                        Begin myBegin = (Begin) myIn;

                        getTransport().send(
                                new OldRound(myBegin.getSeqNum(), getLeader().getNodeId(), myBegin.getRndNumber() + 1),
                                aPacket.getSender());
                    } else {
                        PaxosMessage myResponse = getAcceptorLearner().process(myPropPkt.getOperation());

                        if (myResponse != null) {
                            getTransport().send(myResponse, aPacket.getSender());
                        }
                    }

                    break;
                }

                default: {
                    getLeader().messageReceived(myMessage, aPacket.getSender());
                    break;
                }
            }
        }
    }
}
