package org.dancres.paxos.messages;

import org.dancres.paxos.impl.Leader;
import org.dancres.paxos.impl.LeaderSelection;

import java.net.InetSocketAddress;

public class Accept implements PaxosMessage, LeaderSelection {
    private long _seqNum;
    private long _rndNumber;
    private InetSocketAddress _nodeId;
    
    public Accept(long aSeqNum, long aRndNumber, InetSocketAddress aNodeId) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _nodeId = aNodeId;
    }

    public int getType() {
        return Operations.ACCEPT;
    }

    public InetSocketAddress getNodeId() {
    	return _nodeId;
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
    	return new Long(_seqNum).hashCode() ^ new Long(_rndNumber).hashCode() ^ _nodeId.hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Accept) {
    		Accept myOther = (Accept) anObject;
    		
    		return (_seqNum == myOther._seqNum)&& (_rndNumber == myOther._seqNum) && (_nodeId.equals(myOther._nodeId));  
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Accept: " + _nodeId + ", " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + " ] ";
    }
}
