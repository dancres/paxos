package org.dancres.paxos;

import java.net.InetSocketAddress;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

/**
 * Status indication returned from the state machine for each vote requested
 */
public class Event implements PaxosMessage {
	public interface Reason {
		public static final int DECISION = -1;
		public static final int OTHER_LEADER = -2;
		public static final int VOTE_TIMEOUT = -3;
		public static final int BAD_MEMBERSHIP = -4;
        public static final int OUT_OF_DATE = -5;
	}
	
    private int _result;
    private long _seqNum;
    private Proposal _consolidatedValue;
    private InetSocketAddress _leader;

    public Event(int aResult, long aSeqNum, Proposal aValue, InetSocketAddress aLeader) {
    	assert(aValue != null);
    	
        _result = aResult;
        _seqNum = aSeqNum;
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

    /**
     * @return additional information associated with the reason returned from <code>getResult()</code>.
     */
    public InetSocketAddress getLeader() {
        return _leader;
    }

    public String toString() {
        return "Event: " + _result + ", " + Long.toHexString(_seqNum) + ", " + 
        	_consolidatedValue + ", " + _leader;
    }

	public int getType() {
		return Operations.EVENT;
	}

	public short getClassification() {
		return CLIENT;
	}

	public InetSocketAddress getNodeId() {
		return _leader;
	}
}
