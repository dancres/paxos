package org.dancres.paxos.impl.core.messages;

public class Last implements PaxosMessage {
    private long _seqNum;
    private long _low;
    private long _high;
    private long _rndNumber;
    private byte[] _value;

    public Last(long aSeqNum, long aLowWatermark, long aHighWatermark, long aMostRecentRound, byte[] aValue) {
        _seqNum = aSeqNum;
        _low = aLowWatermark;
        _high = aHighWatermark;
        _rndNumber = aMostRecentRound;
        _value = aValue;
    }

    public int getType() {
        return Operations.LAST;
    }

    public byte[] getValue() {
        return _value;
    }

    public long getRndNumber() {
        return _rndNumber;
    }

    public long getHighWatermark() {
        return _high;
    }

    public long getLowWatermark() {
        return _low;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public String toString() {
        return "Last: " + Long.toHexString(_seqNum) + " " + Long.toHexString(_low) + "->" + Long.toHexString(_high) +
                " [ " + Long.toHexString(_rndNumber) + " ]";
    }
}
