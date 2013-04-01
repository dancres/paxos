package org.dancres.paxos;

import java.net.InetSocketAddress;

public class StateEvent {
    public static final class Reason {
        /**
         * Paxos has agreed a value for the specified instance
         */
        public static final int DECISION = 0;

        /**
         * The AcceptorLearner in this process has become too out of date for recovery.
         */
        public static final int OUT_OF_DATE = 1;

        /**
         * The AcceptorLearner in this process has been updated and is now recovered.
         */
        public static final int UP_TO_DATE = 2;

        private static final String[] _names = {"Decision", "Out of Date", "Up to Date"};

        public static String nameFor(int aCode) {
            if (aCode < 0 || aCode > _names.length - 1)
                throw new IllegalArgumentException("Code not known:" + aCode);

            return _names[aCode];
        }
    }

    private final int _result;
    private final long _seqNum;
    private final long _rndNumber;
    private final Proposal _consolidatedValue;
    private final InetSocketAddress _leader;

    public StateEvent(int aResult, long aSeqNum, long aRndNumber, Proposal aValue,
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
    public int getResult() {
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
        return "VoteOutcome: " + Reason.nameFor(_result) + ", " + Long.toHexString(_seqNum) + ", " + _leader;
    }
}
