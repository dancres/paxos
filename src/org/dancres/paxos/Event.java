package org.dancres.paxos;

import java.nio.ByteBuffer;

/**
 * Status indication returned from the state machine for each vote requested
 */
public class Event {
	public interface Reason {
		public static final int DECISION = 0;
		public static final int OTHER_LEADER = -1;
		public static final int VOTE_TIMEOUT = -2;
		public static final int BAD_MEMBERSHIP = -3;
	}
	
    private int _result;
    private long _seqNum;
    private ConsolidatedValue _consolidatedValue;
    private Object _context;

    Event(int aResult, long aSeqNum, ConsolidatedValue aValue) {
        _result = aResult;
        _seqNum = aSeqNum;
        _consolidatedValue = aValue;
    }

    Event(int aResult, long aSeqNum, ConsolidatedValue aValue, Object aContext) {
        this(aResult, aSeqNum, aValue);
        _context = aContext;
    }

    /**
     * @return the value submitted to the vote
     */
    public byte[] getValue() {
        return _consolidatedValue.getValue();
    }

    public byte[] getHandback() {
        return _consolidatedValue.getHandback();
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
    public Object getContext() {
        return _context;
    }

    public String toString() {
        return "Event: " + _result + ", " + Long.toHexString(_seqNum) + ", " + 
        	_consolidatedValue + ", " + _context;
    }
}
