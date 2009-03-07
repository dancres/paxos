package org.dancres.paxos.impl.core.messages;

import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Operations;

/**
 * Last doesn't need nodeId - if the round is old, round number needs to increase and then it's down to leader
 * number.  If the other leader drops out, then incrementing the round is sufficient.
 */
public class Last implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;
    private byte[] _value;

    public Last(long aSeqNum, long aMostRecentRound, byte[] aValue) {
        _seqNum = aSeqNum;
        _rndNumber = aMostRecentRound;
        _value = aValue;
    }

    public int getType() {
        return Operations.LAST;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public byte[] getValue() {
        return _value;
    }

    public long getRndNumber() {
        return _rndNumber;
    }

    public String toString() {
        return "Last: " + Long.toHexString(_seqNum) + " [ " + Long.toHexString(_rndNumber) + " ]";
    }
}
