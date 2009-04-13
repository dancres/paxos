package org.dancres.paxos.impl.core.messages;

public class Begin implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;
    private long _nodeId;
    private byte[] _value;

    public Begin(long aSeqNum, long aRndNumber, long aNodeId, byte[] aValue) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _value = aValue;
        _nodeId = aNodeId;
    }

    public int getType() {
        return Operations.BEGIN;
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
        return "Begin: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + ", " + Long.toHexString(_nodeId) + " ] ";
    }

    public boolean originates(long aRound, long aNodeId) {
        return ((_rndNumber == aRound) && (_nodeId == aNodeId));
    }

    public boolean precedes(long aRound) {
        return (_rndNumber < aRound);
    }

    public long getNodeId() {
        return _nodeId;
    }
}
