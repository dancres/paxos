package org.dancres.paxos.messages;

import java.util.EnumSet;

public class Learned implements PaxosMessage {
    private final long _seqNum;
    private final long _rndNum;
    
    public Learned(long aSeqNum, long aRndNumber) {
        _seqNum = aSeqNum;
        _rndNum = aRndNumber;
    }

    public int getType() {
        return Types.LEARNED;
    }

    public EnumSet<Classification> getClassifications() {
    	return EnumSet.of(Classification.ACCEPTOR_LEARNER);
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
    	if (anObject instanceof Learned) {
    		Learned myOther = (Learned) anObject;
    		
    		return (_seqNum == myOther._seqNum) && (_rndNum == myOther._rndNum);
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Learned: " + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNum);
    }
}
