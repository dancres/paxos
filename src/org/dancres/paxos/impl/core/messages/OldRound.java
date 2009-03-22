package org.dancres.paxos.impl.core.messages;

import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Operations;

public class OldRound implements PaxosMessage {
    private long _seqNum;
    private long _lastRound;
    private long _nodeId;

    public OldRound(long aSeqNum, long aNodeId, long aLastRound) {
        _seqNum = aSeqNum;
        _lastRound = aLastRound;
    }

    public int getType() {
        return Operations.OLDROUND;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public long getNodeId() {
        return _nodeId;
    }

    public long getLastRound() {
        return _lastRound;
    }

    public String toString() {
        return "OldRound: " + Long.toHexString(_seqNum) + " [ " + Long.toHexString(_lastRound) + " ]";
    }
}
