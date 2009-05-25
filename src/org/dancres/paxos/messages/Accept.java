package org.dancres.paxos.messages;

public class Accept implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;

    public Accept(long aSeqNum, long aRndNumber) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
    }

    public int getType() {
        return Operations.ACCEPT;
    }

    public long getRndNumber() {
        return _rndNumber;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public String toString() {
        return "Accept: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + " ] ";
    }
}
