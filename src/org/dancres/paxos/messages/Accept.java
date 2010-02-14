package org.dancres.paxos.messages;

public class Accept implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;
    private long _nodeId;
    
    public Accept(long aSeqNum, long aRndNumber, long aNodeId) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _nodeId = aNodeId;
    }

    public int getType() {
        return Operations.ACCEPT;
    }

    public long getNodeId() {
    	return _nodeId;
    }
    
    public short getClassification() {
    	return ACCEPTOR_LEARNER;
    }
    
    public long getRndNumber() {
        return _rndNumber;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_rndNumber).hashCode() ^ new Long(_nodeId).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Accept) {
    		Accept myOther = (Accept) anObject;
    		
    		return (_seqNum == myOther._seqNum)&& (_rndNumber == myOther._seqNum) && (_nodeId == myOther._nodeId);  
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Accept: " + Long.toHexString(_nodeId) + ", " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + " ] ";
    }
}
