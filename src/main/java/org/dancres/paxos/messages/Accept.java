package org.dancres.paxos.messages;

import org.dancres.paxos.impl.Instance;
import org.dancres.paxos.impl.LeaderSelection;

import java.util.EnumSet;

public class Accept implements PaxosMessage, LeaderSelection {
    private final long _seqNum;
    private final long _rndNumber;
    
    public Accept(long aSeqNum, long aRndNumber) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
    }

    public int getType() {
        return Operations.ACCEPT;
    }

    public EnumSet<Classification> getClassifications() {
    	return EnumSet.of(Classification.LEADER, Classification.ACCEPTOR_LEARNER);
    }
    
    public long getRndNumber() {
        return _rndNumber;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public boolean routeable(Instance anInstance) {
        return ((_rndNumber == anInstance.getRound()) && (_seqNum == anInstance.getSeqNum()) &&
                (anInstance.getState().equals(Instance.State.SUCCESS)));
    }

    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_rndNumber).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Accept) {
    		Accept myOther = (Accept) anObject;
    		
    		return (_seqNum == myOther._seqNum) && (_rndNumber == myOther._rndNumber);
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Accept: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + " ] ";
    }
}
