package org.dancres.paxos.messages;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.LogStorage;

public class Begin implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;
    private long _nodeId;
    private ConsolidatedValue _consolidatedValue;
    
    public Begin(long aSeqNum, long aRndNumber, ConsolidatedValue aValue, long aNodeId) {
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
    
    public String toString() {
        return "Begin: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + ", " + Long.toHexString(_nodeId) + " ] " + 
                _consolidatedValue.equals(LogStorage.NO_VALUE);
    }

    public boolean originates(Collect aCollect) {
        return ((_rndNumber == aCollect.getRndNumber()) && (_nodeId == aCollect.getNodeId()));
    }

    public boolean precedes(Collect aCollect) {
        return (_rndNumber < aCollect.getRndNumber());
    }

    public long getNodeId() {
        return _nodeId;
    }
}
