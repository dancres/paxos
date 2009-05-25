package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import org.dancres.paxos.impl.core.AcceptorLearner;
import org.dancres.paxos.impl.core.AcceptorLearnerListener;
import org.dancres.paxos.impl.core.Address;
import org.dancres.paxos.impl.core.Completion;
import org.dancres.paxos.impl.core.Leader;
import org.dancres.paxos.impl.core.Operation;
import org.dancres.paxos.impl.core.Reasons;
import org.dancres.paxos.impl.core.Transport;
import org.dancres.paxos.impl.core.messages.Ack;
import org.dancres.paxos.impl.core.messages.Fail;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.io.mina.ProposerPacket;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.impl.io.mina.Post;
import org.dancres.paxos.impl.util.MemoryLogStorage;
import org.dancres.paxos.impl.util.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulates a paxos network process, acting as both Leader and Acceptor/Learner.
 * 
 * @author dan
 */
public class Node implements PacketListener {

    private static Logger _logger = LoggerFactory.getLogger(Node.class);

    private Address _clientAddress;
    private InetSocketAddress _addr;
    private AcceptorLearner _al;
    private Leader _ld;
    private FailureDetectorImpl _fd;
    private Heartbeater _hb;
    private PacketQueue _pq;
    private Transport _tp;

    /**
     * @param anAddr is the address this node should use
     * @param aTransport to use for sending messages
     * @param anUnresponsivenessThreshold is the time after which the failure detector may declare a node dead
     */
    public Node(InetSocketAddress anAddr, Transport aTransport, long anUnresponsivenessThreshold) {
        _addr = anAddr;
        _tp = aTransport;
        _hb = new Heartbeater(_tp);
        _fd = new FailureDetectorImpl(anUnresponsivenessThreshold);
        _al = new AcceptorLearner(new MemoryLogStorage());
        _ld = new Leader(_fd, NodeId.from(_addr), _tp, _al);
        _pq = new PacketQueueImpl(this);
        _al.add(new PacketBridge());
    }

    public void startup() {
        Thread myHeartbeater = new Thread(_hb);
        myHeartbeater.setDaemon(true);
        myHeartbeater.start();
    }

    public PacketQueue getQueue() {
        return _pq;
    }

    /**
     * @todo Remove the ugly hack
     */
    public void deliver(Packet aPacket) throws Exception {
        PaxosMessage myMessage = aPacket.getMsg();

        switch (myMessage.getType()) {
            case Heartbeat.TYPE: {
                _fd.processMessage(myMessage, aPacket.getSender());

                break;
            }

            // UGLY HACK!!!
            //
            case Operations.POST : {
                _clientAddress = aPacket.getSender();
                _ld.submit(new Operation(((Post) myMessage).getValue()));
                break;
            }

            case Operations.PROPOSER_REQ: {
                ProposerPacket myPropPkt = (ProposerPacket) myMessage;
                PaxosMessage myResponse = _al.process(myPropPkt.getOperation());

                if (myResponse != null) {
                    _tp.send(myResponse, aPacket.getSender());
                }

                break;
            }

            default: {
                _ld.messageReceived(myMessage, aPacket.getSender());
                break;
            }
        }
    }

    public FailureDetectorImpl getFailureDetector() {
        return _fd;
    }

    public AcceptorLearner getAcceptorLearner() {
        return _al;
    }

    public Transport getTransport() {
        return _tp;
    }

    public Leader getLeader() {
        return _ld;
    }

    /**
     * @todo Remove this ugly hack once we do handbacks etc
     */
    class PacketBridge implements AcceptorLearnerListener {

        public void done(Completion aCompletion) {
            if (aCompletion.getResult() == Reasons.OK) {
                _tp.send(new Ack(aCompletion.getSeqNum()), _clientAddress);
            } else {
                _tp.send(new Fail(aCompletion.getSeqNum(), aCompletion.getResult()), _clientAddress);
            }
        }
    }
}
