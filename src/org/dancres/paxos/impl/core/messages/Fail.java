package org.dancres.paxos.impl.core.messages;

public class Fail implements PaxosMessage {
    private long _seqNum;

    public Fail(long aSeqNum) {
        _seqNum = aSeqNum;
    }

    public int getType() {
        return Operations.FAIL;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public String toString() {
        return "Fail: " + _seqNum;
    }
}
