package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import org.dancres.paxos.impl.core.AcceptorLearnerState;
import org.dancres.paxos.impl.core.ProposerState;
import org.dancres.paxos.impl.core.Transport;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.ProposerPacket;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.impl.faildet.Heartbeater;
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

    private InetSocketAddress _addr;
    private AcceptorLearnerState _al;
    private ProposerState _ps;
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
        _al = new AcceptorLearnerState(new MemoryLogStorage());
        _ps = new ProposerState(_fd, NodeId.from(_addr), _tp);
        _pq = new PacketQueueImpl(this);
    }

    public void startup() {
        Thread myHeartbeater = new Thread(_hb);
        myHeartbeater.setDaemon(true);
        myHeartbeater.start();
    }

    public PacketQueue getQueue() {
        return _pq;
    }

    public void deliver(Packet aPacket) throws Exception {
        PaxosMessage myMessage = aPacket.getMsg();

        switch (myMessage.getType()) {
            case Heartbeat.TYPE: {
                _fd.processMessage(myMessage, aPacket.getSender());

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
                _ps.process(myMessage, aPacket.getSender());
                break;
            }
        }
    }

    public FailureDetectorImpl getFailureDetector() {
        return _fd;
    }

    public AcceptorLearnerState getAcceptorLearner() {
        return _al;
    }

    public Transport getTransport() {
        return _tp;
    }

    public ProposerState getProposer() {
        return _ps;
    }
}
