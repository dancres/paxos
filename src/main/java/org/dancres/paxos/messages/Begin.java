package org.dancres.paxos.messages;

import org.dancres.paxos.Proposal;

import java.util.EnumSet;

public class Begin implements Claim {
    private final long _seqNum;
    private final long _rndNumber;
    private final Proposal _consolidatedValue;
    
    public Begin(long aSeqNum, long aRndNumber, Proposal aValue) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _consolidatedValue = aValue;
    }

    public int getType() {
        return Types.BEGIN;
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

    public Proposal getConsolidatedValue() {
    	return _consolidatedValue;
    }
    
    public int hashCode() {
    	return Long.valueOf(_seqNum).hashCode() ^ Long.valueOf(_rndNumber).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Begin) {
    		Begin myOther = (Begin) anObject;
    		
    		return (_seqNum == myOther._seqNum) &&
                    (_rndNumber == myOther._rndNumber);
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Begin: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + " value = " +
                ! _consolidatedValue.equals(Proposal.NO_VALUE) + " ]";
    }
}
