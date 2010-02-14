package org.dancres.paxos.messages;

public class OldRound implements PaxosMessage {
    private long _seqNum;
    private long _lastRound;
    private long _leaderNodeId;
    private long _nodeId;
    
    public OldRound(long aSeqNum, long aLeaderNodeId, long aLastRound, long aNodeId) {
        _seqNum = aSeqNum;
        _leaderNodeId = aLeaderNodeId;
        _lastRound = aLastRound;
        _nodeId = aNodeId;
    }

    public int getType() {
        return Operations.OLDROUND;
    }

    public short getClassification() {
    	return ACCEPTOR_LEARNER;
    }
    
    public long getSeqNum() {
        return _seqNum;
    }

    public long getLeaderNodeId() {
        return _leaderNodeId;
    }

    public long getNodeId() {
    	return _nodeId;
    }
        
    public long getLastRound() {
        return _lastRound;
    }

    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_lastRound).hashCode() ^ new Long(_leaderNodeId).hashCode() ^
    		new Long(_nodeId).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof OldRound) {
    		OldRound myOther = (OldRound) anObject;
    		
    		return (myOther._seqNum == _seqNum) && (myOther._lastRound == _lastRound) && 
    			(myOther._leaderNodeId == _leaderNodeId) && (myOther._nodeId == _nodeId);
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "OldRound: " + Long.toHexString(_seqNum) + " [ " + Long.toHexString(_lastRound) + ", " +
                Long.toHexString(_leaderNodeId) + " ]";
    }
}
