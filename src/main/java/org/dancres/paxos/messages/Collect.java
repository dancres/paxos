package org.dancres.paxos.messages;

import org.dancres.paxos.impl.Constants;

import java.util.EnumSet;

public class Collect implements PaxosMessage {
    private final long _seqNum;
    private final long _rndNumber;

    public static final Collect INITIAL = new Collect(Constants.UNKNOWN_SEQ, Long.MIN_VALUE);

    public Collect(long aSeqNum, long aRndNumber) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
    }

    public int getType() {
        return Operations.COLLECT;
    }
    
    public EnumSet<Classification> getClassifications() {
    	return EnumSet.of(Classification.ACCEPTOR_LEARNER);
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public long getRndNumber() {
        return _rndNumber;
    }

    public String toString() {
        return "Collect: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + " ] ";
    }

    public boolean isInitial() {
    	return this.equals(INITIAL);
    }
    
    public int hashCode() {
    	return new Long(_rndNumber).hashCode() ^ new Long(_seqNum).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Collect) {
    		Collect myOther = (Collect) anObject;
    		
    		return ((myOther._rndNumber == _rndNumber) && 
    				(myOther._seqNum == _seqNum)); 
    	}
    	
    	return false;
    }
}
