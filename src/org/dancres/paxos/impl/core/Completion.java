package org.dancres.paxos.impl.core;

public class Completion {
    private int _result;
    private long _seqNum;
    private byte[] _value;

    Completion(int aResult, long aSeqNum, byte[] aValue) {
        _result = aResult;
        _seqNum = aSeqNum;
        _value = aValue;
    }

    public byte[] getValue() {
        return _value;
    }

    public int getResult() {
        return _result;
    }

    public long getSeqNum() {
        return _seqNum;
    }
}
