package org.dancres.paxos.messages;

import org.dancres.paxos.LogStorage;

public class Collect implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;
    private long _nodeId;

    public static final Collect INITIAL = new Collect(LogStorage.NO_SEQ, Long.MIN_VALUE, Long.MIN_VALUE);

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

    public boolean sameLeader(Collect aCollect) {
    	return ((_rndNumber == aCollect._rndNumber) && (_nodeId == aCollect._nodeId));
    }
    
    public boolean isInitial() {
    	return equals(INITIAL);
    }
    
    public int hashCode() {
    	return new Long(_nodeId).hashCode() ^ new Long(_rndNumber).hashCode() ^ new Long(_seqNum).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Collect) {
    		Collect myOther = (Collect) anObject;
    		
    		return ((myOther._nodeId == _nodeId) && (myOther._rndNumber == _rndNumber) && 
    				(myOther._seqNum == _seqNum)); 
    	}
    	
    	return false;
    }
}
