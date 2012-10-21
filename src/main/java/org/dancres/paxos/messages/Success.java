package org.dancres.paxos.messages;

import org.dancres.paxos.Proposal;

import java.net.InetSocketAddress;

public class Success implements PaxosMessage {
    private long _seqNum;
    private long _rndNum;
    
    public Success(long aSeqNum, long aRndNumber) {
        _seqNum = aSeqNum;
        _rndNum = aRndNumber;
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
    
    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_rndNum).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Success) {
    		Success myOther = (Success) anObject;
    		
    		return (_seqNum == myOther._seqNum) && (_rndNum == myOther._rndNum);
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Success: " + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNum);
    }
}
