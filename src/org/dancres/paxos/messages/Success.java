package org.dancres.paxos.messages;

import org.dancres.paxos.ConsolidatedValue;

public class Success implements PaxosMessage {
    private long _seqNum;
    private ConsolidatedValue _value;

    public Success(long aSeqNum, ConsolidatedValue aValue) {
        _seqNum = aSeqNum;
        _value = aValue;
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

    public ConsolidatedValue getConsolidatedValue() {
        return _value;
    }
    
    public String toString() {
        return "Success: " + _seqNum;
    }
}
