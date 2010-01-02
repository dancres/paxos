package org.dancres.paxos.messages;

import org.dancres.paxos.LogStorage;

public class Collect implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;
    private long _nodeId;

    public static final Collect INITIAL = new Collect(0, LogStorage.EMPTY_LOG, Long.MIN_VALUE);

    public Collect(long aSeqNum, long aRndNumber, long aNodeId) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _nodeId = aNodeId;
    }

    public int getType() {
        return Operations.COLLECT;
    }
    
    public short getClassification() {
    	return LEADER;
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

    public boolean supercedes(Collect aCollect) {
        return (_rndNumber > aCollect.getRndNumber());
    }

    public boolean isInitial() {
        return (_nodeId == 0);
    }
}
