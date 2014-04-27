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
        VALUE,

        /**
         * This process is now lagging too far behind the others in the paxos cluster and must be brought back into
         * alignment via a suitable checkpoint.
         */
        OUT_OF_DATE,

        /**
         * This process is no longer lagging behind the paxos cluster and will report decisions accordingly.
         */
        UP_TO_DATE,

        /**
         * A new leader has been elected
         */
        NEW_LEADER
    }

    private final Reason _result;
    private final long _seqNum;
    private final long _rndNumber;
    private final Proposal _consolidatedValue;
    private final InetSocketAddress _leader;
    private final byte[] _leaderId;

    public StateEvent(Reason aResult, long aSeqNum, long aRndNumber, Proposal aValue, byte[] aLeaderId,
                       InetSocketAddress aLeader) {
        assert(aValue != null);

        _result = aResult;
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _consolidatedValue = aValue;
        _leaderId = aLeaderId;
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
     * @return the address of the Paxos leader that issued the update.
     */
    public InetSocketAddress getLeaderAddress() {
        return _leader;
    }

    /**
     * @return the user-code identifier for the leader that issued the update if it is available.
     */
    public byte[] getLeaderId() {
        return _leaderId;
    }

    public String toString() {
        return "VoteOutcome: " + _result.name() + ", " + Long.toHexString(_seqNum) + ", " + _leaderId + ", " + _leader;
    }
}
