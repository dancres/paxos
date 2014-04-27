package org.dancres.paxos.impl;

/**
 * Tracks the last contiguous sequence number for which an AL has a value.
 *
 * When we receive a success, if it's seqNum is this field + 1, increment
 * this field. Acts as the low watermark for leader recovery, essentially we
 * want to recover from the last contiguous sequence number in the stream of
 * paxos instances.
 */
class Watermark implements Comparable<Watermark> {
    static final Watermark INITIAL = new Watermark(Constants.UNKNOWN_SEQ, -1);
    private final long _seqNum;
    private final long _logOffset;

    /**
     * @param aSeqNum the current sequence
     * @param aLogOffset the log offset of the success record for this sequence number
     */
    Watermark(long aSeqNum, long aLogOffset) {
        _seqNum = aSeqNum;
        _logOffset = aLogOffset;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public long getLogOffset() {
        return _logOffset;
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof Watermark) {
            Watermark myOther = (Watermark) anObject;

            return (myOther._seqNum == _seqNum) && (myOther._logOffset == _logOffset);
        }

        return false;
    }

    public int compareTo(Watermark aWatermark) {
        if (aWatermark.getSeqNum() == _seqNum)
            return 0;
        else if (aWatermark.getSeqNum() < _seqNum)
            return 1;
        else
            return -1;
    }

    public String toString() {
        return "Watermark: " + Long.toHexString(_seqNum) + ", " + Long.toHexString(_logOffset);
    }
}

