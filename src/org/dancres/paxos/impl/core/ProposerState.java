package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.OldRound;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;
import org.dancres.paxos.impl.core.messages.Motion;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Post;

/**
 * @author dan
 */
public class ProposerState {
    private long _nodeId;

    private Logger _logger = LoggerFactory.getLogger(ProposerState.class);

    private FailureDetector _fd;

    private long _rndNumber = 0;

    private LeaderImpl _leader;

    /**
     * Note that being the leader is merely an optimisation and saves on sending COLLECTs.  Thus if one thread establishes we're leader and
     * a prior thread decides otherwise with the latter being last to update this variable we'll simply do an unnecessary COLLECT.  The
     * protocol execution will still be correct.
     */
    private boolean _isLeader = false;

    /**
     * @param aDetector to maintain for use by proposers
     */
    ProposerState(FailureDetector aDetector, long aNodeId, Transport aTransport) {
        _fd = aDetector;
        _nodeId = aNodeId;
        _leader = new LeaderImpl(this, aTransport);

        _logger.info("Initialized state with id: " + Long.toHexString(_nodeId));
    }

    long newRndNumber() {
        synchronized(this) {
            return ++_rndNumber;
        }
    }

    public FailureDetector getFailureDetector() {
        return _fd;
    }

    void updateRndNumber(OldRound anOldRound) {
        synchronized(this) {
            _rndNumber = anOldRound.getLastRound() + 1;
        }
    }

    boolean isLeader() {
        synchronized(this) {
            return _isLeader;
        }
    }

    void amLeader() {
        synchronized(this) {
            _isLeader = true;
        }
    }

    void notLeader() {
        synchronized(this) {
            _isLeader = false;
        }
    }

    public long getRndNumber() {
        synchronized(this) {
            return _rndNumber;
        }
    }

    public long getNodeId() {
        return _nodeId;
    }

    /**
     * @param aMessage to process
     * @param aSenderAddress at which the sender of this message can be found
     */
    public void process(PaxosMessage aMessage, Address aSenderAddress) {
        switch (aMessage.getType()) {
            case Operations.POST : {
                _logger.info("Received post - starting leader");

                Post myPost = (Post) aMessage;
                _leader.messageReceived(new Motion(myPost.getValue()), aSenderAddress);

                break;
            }

            case Operations.OLDROUND :
            case Operations.LAST :
            case Operations.ACCEPT :
            case Operations.ACK: {
                _leader.messageReceived(aMessage, aSenderAddress);
                break;
            }
            default : throw new RuntimeException("Invalid message: " + aMessage.getType());
        }
    }
}
