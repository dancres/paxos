package org.dancres.paxos;

import java.net.InetSocketAddress;

/**
 * Status indication returned from the state machine for each vote requested
 */
public class VoteOutcome {
	public static final class Reason {
        /**
         * Paxos has agreed a value for the specified instance
         */
		public static final int DECISION = 0;

        /**
         * The process attempting to lead an instance has found another leader active and given-up. Round and instance
         * specify the current state of the paxos sequence of instances this node is participating in as viewed by
         * the AL that thinks there is another leader.
         */
		public static final int OTHER_LEADER = 1;

        /**
         * Insufficient responses were received for this leader to continue to progress this instance.
         */
		public static final int VOTE_TIMEOUT = 2;

        /**
         * This leader's view of membership has become sufficiently unhealthy that progress of this instance is
         * impossible.
         */
		public static final int BAD_MEMBERSHIP = 3;

        /**
         * The Leader in this process has received a counter-proposal for the current paxos instance.
         * The sequence number and round associated with the instance are present in the outcome.
         */
        public static final int OTHER_VALUE = 4;
        
        private static final String[] _names = {"Decision", "Other Leader", "Vote Timeout", "Bad Membership",
        	"Other Value"};
        
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

    public VoteOutcome(int aResult, long aSeqNum, long aRndNumber, Proposal aValue,
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
