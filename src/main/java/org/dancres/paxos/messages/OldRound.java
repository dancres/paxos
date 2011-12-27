package org.dancres.paxos.messages;

import org.dancres.paxos.impl.Leader;
import org.dancres.paxos.impl.LeaderSelection;

import java.net.InetSocketAddress;

/**
 * OldRound always indicates a leader is not in charge regardless of sequence number, thus it needn't be attached to a
 * specific sequence number and thus OldRound can contain most recent successful sequence number not the original
 */
public class OldRound implements PaxosMessage, LeaderSelection {
    private long _seqNum;
    private long _lastRound;
    private InetSocketAddress _leaderNodeId;
    private InetSocketAddress _nodeId;
    
    public OldRound(long aSeqNum, InetSocketAddress aLeaderNodeId, long aLastRound, InetSocketAddress aNodeId) {
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

    public InetSocketAddress getLeaderNodeId() {
        return _leaderNodeId;
    }

    public InetSocketAddress getNodeId() {
    	return _nodeId;
    }
        
    public long getLastRound() {
        return _lastRound;
    }

    public boolean routeable(Leader aLeader) {
        return ((_lastRound >= aLeader.getRound()) &&
                ((aLeader.getState().equals(Leader.States.BEGIN)) ||
                 (aLeader.getState().equals(Leader.States.SUCCESS))));
    }

    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_lastRound).hashCode() ^ _leaderNodeId.hashCode() ^
    		_nodeId.hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof OldRound) {
    		OldRound myOther = (OldRound) anObject;
    		
    		return (myOther._seqNum == _seqNum) && (myOther._lastRound == _lastRound) && 
    			(myOther._leaderNodeId.equals(_leaderNodeId)) && (myOther._nodeId.equals(_nodeId));
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "OldRound: " + Long.toHexString(_seqNum) + " [ " + Long.toHexString(_lastRound) + ", " +
                _leaderNodeId + " ]";
    }
}
