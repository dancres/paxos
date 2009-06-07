package org.dancres.paxos;

/**
 * Status indication returned from the state machine for each vote requested
 */
public class Completion {
    private int _result;
    private long _seqNum;
    private byte[] _value;
    private Object _context;

    Completion(int aResult, long aSeqNum, byte[] aValue) {
        _result = aResult;
        _seqNum = aSeqNum;
        _value = aValue;
    }

    Completion(int aResult, long aSeqNum, byte[] aValue, Object aContext) {
        this(aResult, aSeqNum, aValue);
        _context = aContext;
    }

    /**
     * @return the value submitted to the vote
     */
    public byte[] getValue() {
        return _value;
    }

    /**
     * @return the completion code for a requested vote, one of {@link Reasons}
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
        return "Completion: " + _result + ", " + Long.toHexString(_seqNum) + ", " + _context;
    }
}
