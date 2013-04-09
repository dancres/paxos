package org.dancres.paxos;

import java.net.InetSocketAddress;

/**
 * State updates for the persistent log.
 */
public class StateEvent {
    public enum Reason {
        /**
         * Paxos has agreed a value for the specified instance. The reported value should be applied to the
         * system state (after which an optional checkpoint could be requested).
         */
        DECISION,

        /**
         * This process is now lagging too far behind the others in the paxos cluster and must be brought back into
         * alignment via a suitable checkpoint.
         */
        OUT_OF_DATE,

        /**
         * This process is no longer lagging behind the paxos cluster and will report decisions accordingly.
         */
        UP_TO_DATE
    };

    private final Reason _result;
    private final long _seqNum;
    private final long _rndNumber;
    private final Proposal _consolidatedValue;
    private final InetSocketAddress _leader;

    public StateEvent(Reason aResult, long aSeqNum, long aRndNumber, Proposal aValue,
                       InetSocketAddress aLeader) {
        assert(aValue != null);

        _result = aResult;
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _consolidatedValue = aValue;
        _leader = aLeader;
    }

    public Proposal getValues() {
        return _consolidatedValue;
    }

    /**
     * @return the completion code for a requested vote, one of {@link Reason}
     */
    public Reason getResult() {
        return _result;
    }

    /**
     * @return the sequence number associated with the vote, if any
     */
    public long getSeqNum() {
        return _seqNum;
    }

    public long getRndNumber() {
        return _rndNumber;
    }

    /**
     * @return additional information associated with the reason returned from <code>getResult()</code>.
     */
    public InetSocketAddress getLeader() {
        return _leader;
    }

    public String toString() {
        return "VoteOutcome: " + _result.name() + ", " + Long.toHexString(_seqNum) + ", " + _leader;
    }
}
