package org.dancres.paxos.messages;

public class Success implements PaxosMessage {
    private long _seqNum;
    private byte[] _value;

    public Success(long aSeqNum, byte[] aValue) {
        _seqNum = aSeqNum;
        _value = aValue;
    }

    public int getType() {
        return Operations.SUCCESS;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public byte[] getValue() {
        return _value;
    }
    
    public String toString() {
        return "Success: " + _seqNum;
    }
}
