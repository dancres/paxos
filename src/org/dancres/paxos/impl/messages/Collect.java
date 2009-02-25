package org.dancres.paxos.impl.messages;

public class Collect implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;
    private long _nodeId;

    public Collect(long aSeqNum, long aRndNumber, long aNodeId) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _nodeId = aNodeId;
    }

    public int getType() {
        return Operations.COLLECT;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public long getRndNumber() {
        return _rndNumber;
    }

    public long getNodeId() {
        return _nodeId;
    }

    public String toString() {
        return "Collect: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + ", " + Long.toHexString(_nodeId) + " ] ";
    }

    public boolean supercedes(long aLastRound, long aLastNodeId) {
        if (_rndNumber > aLastRound)
            return true;
        else if ((_rndNumber == aLastRound) && (_nodeId > aLastNodeId))
            return true;
        else
            return false;
    }
}
