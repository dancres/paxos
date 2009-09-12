package org.dancres.paxos;

import java.nio.ByteBuffer;

/**
 * Status indication returned from the state machine for each vote requested
 */
public class Completion {
    private int _result;
    private long _seqNum;
    private byte[] _value;
    private byte[] _handback;
    private Object _context;

    Completion(int aResult, long aSeqNum, byte[] aValue) {
        _result = aResult;
        _seqNum = aSeqNum;

        if (aValue != null) {
            ByteBuffer myBuffer = ByteBuffer.wrap(aValue);
            int myValueSize = myBuffer.getInt();

            _value = new byte[myValueSize];
            _handback = new byte[aValue.length - 4 - myValueSize];
            myBuffer.get(_value);
            myBuffer.get(_handback);
        }
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

    public byte[] getHandback() {
        return _handback;
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
