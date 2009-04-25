package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.OldRound;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dancres.paxos.impl.faildet.FailureDetector;

import java.util.Map;
import java.util.TreeMap;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

/**
 * @todo Remove the need for _addr in this class - that's the transport leaking into the core library and we wish to avoid it.
 * This is as the result of having the Leader construct the ProposerHeader however it's decoded and used inside of the
 * AcceptorLearnerAdaptor only.  Thus we should move the wrapping with a ProposerHeader into the transport implementation
 * which can consider the type of message and wrap it (with the port) as required.
 * 
 * @author dan
 */
public class ProposerState {
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
     * Note that being the leader is merely an optimisation and saves on sending COLLECTs.  Thus if one thread establishes we're leader and
     * a prior thread decides otherwise with the latter being last to update this variable we'll simply do an unnecessary COLLECT.  The
     * protocol execution will still be correct.
     */
    private boolean _isLeader = false;

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

    void dispose(long aSeqNum) {
        synchronized(this) {
            _activeRounds.remove(new Long(aSeqNum));
        }
    }

    /**
     * Create a leader instance for this round
     *
     * @param aChannel is the broadcast channel the leader should use to communicate with acceptor/learners
     * @param aClientChannel is the channel to send the outcome to when leader is done
     * @return
     */
    private LeaderImpl newLeader(Transport aTransport, Address aClientAddress) {
        synchronized(this) {
            long mySeqNum = getNextSeqNum();

            LeaderImpl myLeader = new LeaderImpl(mySeqNum, this, aTransport, aClientAddress);
            _activeRounds.put(new Long(mySeqNum), myLeader);

            return myLeader;
        }
    }

    private LeaderImpl getLeader(long aSeqNum) {
        synchronized(this) {
            return (LeaderImpl) _activeRounds.get(new Long(aSeqNum));
        }
    }

    /**
     * @param aMessage to process
     * @param aSenderAddress at which the sender of this message can be found
     */
    public void process(PaxosMessage aMessage, Address aSenderAddress, Transport aTransport) {
        switch (aMessage.getType()) {
            case Operations.POST : {
                _logger.info("Received post - starting leader");

                // Sender address will be the client
                //
                LeaderImpl myLeader = newLeader(aTransport, aSenderAddress);
                myLeader.messageReceived(aMessage, aSenderAddress);
                break;
            }

            case Operations.OLDROUND :
            case Operations.LAST :
            case Operations.ACCEPT :
            case Operations.ACK: {
                LeaderImpl myLeader = getLeader(aMessage.getSeqNum());

                if (myLeader != null)
                    myLeader.messageReceived(aMessage, aSenderAddress);
                else {
                    _logger.warn("Leader not present for: " + aMessage);
                }

                break;
            }
            default : throw new RuntimeException("Invalid message: " + aMessage.getType());
        }
    }
}
