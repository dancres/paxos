package org.dancres.paxos.messages;

public class Last implements PaxosMessage {
    private long _seqNum;
    private long _low;
    private long _high;
    private long _rndNumber;
    private byte[] _value;

    /**
     * @param aSeqNum is the sequence number received in the related collect
     * @param aLowWatermark is the last contiguous sequence number seen
     * @param aHighWatermark is the most recent sequence number seen
     * @param aMostRecentRound is the most recent leader round seen
     * @param aValue is the value, if any, associated with the sequence number of the related collect
     */
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

    /**
     * @return the value associated with the sequence number returned by <code>getSeqNum</code>
     */
    public byte[] getValue() {
        return _value;
    }

    /**
     * @return the most recent leader round seen
     */
    public long getRndNumber() {
        return _rndNumber;
    }

    /**
     * @return the most recent sequence number seen
     */
    public long getHighWatermark() {
        return _high;
    }

    /**
     * @return the last contiguous sequence number seen
     */
    public long getLowWatermark() {
        return _low;
    }

    /**
     * @return the sequence number received in the related collect
     */
    public long getSeqNum() {
        return _seqNum;
    }

    public String toString() {
        return "Last: " + Long.toHexString(_seqNum) + " " + Long.toHexString(_low) + "->" + Long.toHexString(_high) +
                " [ " + Long.toHexString(_rndNumber) + " ]";
    }
}
