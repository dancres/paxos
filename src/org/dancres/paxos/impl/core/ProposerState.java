package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.OldRound;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dancres.paxos.impl.faildet.FailureDetector;

import java.util.Map;
import java.util.TreeMap;
import java.net.InetSocketAddress;
import org.dancres.paxos.impl.util.NodeId;

/**
 * @todo Remove the need for _addr in this class - that's the transport leaking into the core library and we wish to avoid it.
 * This is as the result of having the Leader construct the ProposerHeader however it's decoded and used inside of the
 * AcceptorLearnerAdaptor only.  Thus we should move the wrapping with a ProposerHeader into the transport implementation
 * which can consider the type of message and wrap it (with the port) as required.
 * 
 * @author dan
 */
class ProposerState {
    /**
     * The next entry in the ledgers we will try and fill - aka log number
     */
    private long _nextSeqNum = 0;
    private Map _activeRounds = new TreeMap();

    private long _nodeId;

    private Logger _logger = LoggerFactory.getLogger(ProposerState.class);

    private FailureDetector _fd;

    private long _rndNumber = 0;

    /**
     * @param aDetector to maintain for use by proposers
     */
    ProposerState(FailureDetector aDetector, long aNodeId) {
        _fd = aDetector;
        _nodeId = aNodeId;

        _logger.info("Initialized state with id: " + Long.toHexString(_nodeId));
    }

    private long getNextSeqNum() {
        return _nextSeqNum++;
    }

    FailureDetector getFailureDetector() {
        return _fd;
    }

    /**
     * Create a leader instance for this round
     *
     * @param aChannel is the broadcast channel the leader should use to communicate with acceptor/learners
     * @param aClientChannel is the channel to send the outcome to when leader is done
     * @return
     */
    LeaderImpl newLeader(Transport aTransport, Address aClientAddress) {
        synchronized(this) {
            long mySeqNum = getNextSeqNum();

            LeaderImpl myLeader = new LeaderImpl(mySeqNum, this, aTransport, aClientAddress);
            _activeRounds.put(new Long(mySeqNum), myLeader);

            return myLeader;
        }
    }

    void updateRndNumber(OldRound anOldRound) {
        synchronized(this) {
            _rndNumber = anOldRound.getLastRound() + 1;
        }
    }

    long getRndNumber() {
        synchronized(this) {
            return _rndNumber;
        }
    }

    long getNodeId() {
        return _nodeId;
    }

    void dispose(long aSeqNum) {
        synchronized(this) {
            _activeRounds.remove(new Long(aSeqNum));
        }
    }

    LeaderImpl getLeader(long aSeqNum) {
        synchronized(this) {
            return (LeaderImpl) _activeRounds.get(new Long(aSeqNum));
        }
    }
}
