package org.dancres.paxos.impl.core.messages;

import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Operations;

/**
 * OldRound doesn't need nodeId - if the round is old, round number needs to increase and then it's down to leader
 * number.  If the other leader drops out, then incrementing the round is sufficient.
 */
public class OldRound implements PaxosMessage {
    private long _seqNum;
    private long _lastRound;

    public OldRound(long aSeqNum, long aLastRound) {
        _seqNum = aSeqNum;
        _lastRound = aLastRound;
    }

    public int getType() {
        return Operations.OLDROUND;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public long getLastRound() {
        return _lastRound;
    }

    public String toString() {
        return "OldRound: " + Long.toHexString(_seqNum) + " [ " + Long.toHexString(_lastRound) + " ]";
    }
}
