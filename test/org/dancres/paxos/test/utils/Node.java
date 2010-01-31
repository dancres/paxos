package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import org.dancres.paxos.AcceptorLearner;
import org.dancres.paxos.AcceptorLearnerListener;
import org.dancres.paxos.Event;
import org.dancres.paxos.Leader;
import org.dancres.paxos.LogStorage;
import org.dancres.paxos.Transport;
import org.dancres.paxos.messages.Complete;
import org.dancres.paxos.messages.Fail;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.faildet.Heartbeater;
import org.dancres.paxos.impl.util.MemoryLogStorage;
import org.dancres.paxos.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emulates a paxos network process, acting as both Leader and Acceptor/Learner.
 * 
 * @author dan
 */
public class Node implements PacketListener {

    private static Logger _logger = LoggerFactory.getLogger(Node.class);

    private NodeId _clientAddress;
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
    	this(anAddr, aTransport, anUnresponsivenessThreshold, new MemoryLogStorage());
    }

    public Node(InetSocketAddress anAddr, Transport aTransport, long anUnresponsivenessThreshold, LogStorage aLogger) {
        _addr = anAddr;
        _tp = aTransport;
        _hb = new Heartbeater(NodeId.from(_addr), _tp);
        _fd = new FailureDetectorImpl(anUnresponsivenessThreshold);
        _al = new AcceptorLearner(aLogger, _tp, NodeId.from(_addr));
        _ld = new Leader(_fd, NodeId.from(_addr), _tp, _al);
        _pq = new PacketQueueImpl(this);
        _al.add(new PacketBridge());    	
    }
    
    public void startup() {
        Thread myHeartbeater = new Thread(_hb);
        myHeartbeater.setDaemon(true);
        myHeartbeater.start();
    }

    public void stop() {
    	_al.close();
    }
    
    public PacketQueue getQueue() {
        return _pq;
    }

    /**
     * @todo Remove the ugly hack
     */
    public void deliver(PaxosMessage aMessage) throws Exception {
        switch (aMessage.getClassification()) {
            case PaxosMessage.FAILURE_DETECTOR : {
                _fd.processMessage(aMessage);

                break;
            }

            // UGLY HACK!!!
            //
            case PaxosMessage.CLIENT : {
                _clientAddress = NodeId.from(aMessage.getNodeId());
                _ld.messageReceived(aMessage);
                break;
            }

            case PaxosMessage.LEADER: {
                _al.messageReceived(aMessage);

                break;
            }

            case PaxosMessage.ACCEPTOR_LEARNER: {
                _ld.messageReceived(aMessage);
                break;
            }
            
            default : {
            	_logger.error("Unrecognised message:" + aMessage);
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

        public void done(Event anEvent) {
            // If we're not the originating node for the post, because we're not leader, we won't have an addressed stored up
            //
            if (_clientAddress == null)
                return;

            if (anEvent.getResult() == Event.Reason.DECISION) {
                _tp.send(new Complete(anEvent.getSeqNum()), _clientAddress);
            } else {
                _tp.send(new Fail(anEvent.getSeqNum(), anEvent.getResult()), _clientAddress);
            }
        }
    }
}
