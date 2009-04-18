package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import org.dancres.paxos.impl.core.AcceptorLearnerImpl;
import org.dancres.paxos.impl.core.ProposerImpl;
import org.dancres.paxos.impl.core.Transport;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.ProposerPacket;
import org.dancres.paxos.impl.faildet.FailureDetector;
import org.dancres.paxos.impl.faildet.Heartbeater;
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
    private AcceptorLearnerImpl _al;
    private ProposerImpl _pi;
    private FailureDetector _fd;
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
        _fd = new FailureDetector(anUnresponsivenessThreshold);
        _al = new AcceptorLearnerImpl();
        _pi = new ProposerImpl(_tp, _fd, NodeId.from(_addr));
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
            case Operations.HEARTBEAT: {
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
                _pi.process(myMessage, aPacket.getSender());
                break;
            }
        }
    }

    public FailureDetector getFailureDetector() {
        return _fd;
    }

    public AcceptorLearnerImpl getAcceptorLearner() {
        return _al;
    }

    public Transport getTransport() {
        return _tp;
    }

    public ProposerImpl getProposer() {
        return _pi;
    }
}
