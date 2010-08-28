package org.dancres.paxos.messages;

import org.dancres.paxos.ConsolidatedValue;

import java.net.InetSocketAddress;

public class Success implements PaxosMessage {
    private long _seqNum;
    private long _rndNum;
    private InetSocketAddress _nodeId;
    
    private ConsolidatedValue _value;

    public Success(long aSeqNum, long aRndNumber, ConsolidatedValue aValue, InetSocketAddress aNodeId) {
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
    
    public InetSocketAddress getNodeId() {
    	return _nodeId;
    }
    
    public ConsolidatedValue getConsolidatedValue() {
        return _value;
    }
    
    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_rndNum).hashCode() ^ _nodeId.hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Success) {
    		Success myOther = (Success) anObject;
    		
    		return (_seqNum == myOther._seqNum) && (_rndNum == myOther._rndNum) && (_nodeId.equals(myOther._nodeId));
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Success: " + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNum) + ", " + 
        	_nodeId;
    }
}
