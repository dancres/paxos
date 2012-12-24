package org.dancres.paxos.messages;

import org.dancres.paxos.impl.Leader;
import org.dancres.paxos.impl.LeaderSelection;

public class Accept implements PaxosMessage, LeaderSelection {
    private long _seqNum;
    private long _rndNumber;
    
    public Accept(long aSeqNum, long aRndNumber) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
    }

    public int getType() {
        return Operations.ACCEPT;
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

    public boolean routeable(Leader aLeader) {
        return ((_seqNum == aLeader.getSeqNum()) && (aLeader.getState().equals(Leader.States.SUCCESS)));
    }

    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_rndNumber).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Accept) {
    		Accept myOther = (Accept) anObject;
    		
    		return (_seqNum == myOther._seqNum) && (_rndNumber == myOther._seqNum);  
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Accept: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + " ] ";
    }
}
