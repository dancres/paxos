package org.dancres.paxos.messages;

public class Fail implements PaxosMessage {
    private long _seqNum;
    private int _reason;

    public Fail(long aSeqNum, int aReason) {
        _seqNum = aSeqNum;
        _reason = aReason;
    }

    public int getType() {
        return Operations.FAIL;
    }

    public short getClassification() {
    	return CLIENT;
    }
    
    public long getSeqNum() {
        return _seqNum;
    }

    public int getReason() {
        return _reason;
    }

    public String toString() {
        return "Fail: " + _seqNum;
    }
}
