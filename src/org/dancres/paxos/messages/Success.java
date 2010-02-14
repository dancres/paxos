package org.dancres.paxos.messages;

import org.dancres.paxos.ConsolidatedValue;

public class Success implements PaxosMessage {
    private long _seqNum;
    private long _rndNum;
    private long _nodeId;
    
    private ConsolidatedValue _value;

    public Success(long aSeqNum, long aRndNumber, ConsolidatedValue aValue, long aNodeId) {
        _seqNum = aSeqNum;
        _rndNum = aRndNumber;
        _value = aValue;
        _nodeId = aNodeId;
    }

    public int getType() {
        return Operations.SUCCESS;
    }

    public short getClassification() {
    	return LEADER;
    }
    
    public long getSeqNum() {
        return _seqNum;
    }

    public long getRndNum() {
    	return _rndNum;
    }
    
    public long getNodeId() {
    	return _nodeId;
    }
    
    public ConsolidatedValue getConsolidatedValue() {
        return _value;
    }
    
    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_rndNum).hashCode() ^ new Long(_nodeId).hashCode(); 
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Success) {
    		Success myOther = (Success) anObject;
    		
    		return (_seqNum == myOther._seqNum) && (_rndNum == myOther._rndNum) && (_nodeId == myOther._nodeId);
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Success: " + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNum) + ", " + 
        	Long.toHexString(_nodeId);
    }
}
