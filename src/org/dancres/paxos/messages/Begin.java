package org.dancres.paxos.messages;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.impl.LogStorage;

import java.net.InetSocketAddress;

public class Begin implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;
    private InetSocketAddress _nodeId;
    private ConsolidatedValue _consolidatedValue;
    
    public Begin(long aSeqNum, long aRndNumber, ConsolidatedValue aValue, InetSocketAddress aNodeId) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _nodeId = aNodeId;
        _consolidatedValue = aValue;
    }

    public int getType() {
        return Operations.BEGIN;
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

    public ConsolidatedValue getConsolidatedValue() {
    	return _consolidatedValue;
    }
    
    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_rndNumber).hashCode() ^ _nodeId.hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Begin) {
    		Begin myOther = (Begin) anObject;
    		
    		return (_seqNum == myOther._seqNum) &&
                    (_rndNumber == myOther._rndNumber) && (_nodeId.equals(myOther._nodeId));
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Begin: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + ", " + _nodeId + " ] " +
                _consolidatedValue.equals(LogStorage.NO_VALUE);
    }

    public boolean originates(Collect aCollect) {
        return ((_rndNumber == aCollect.getRndNumber()) && (_nodeId.equals(aCollect.getNodeId())));
    }

    public boolean precedes(Collect aCollect) {
        return (_rndNumber < aCollect.getRndNumber());
    }

    public InetSocketAddress getNodeId() {
        return _nodeId;
    }
}
