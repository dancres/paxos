package org.dancres.paxos.messages;

import org.dancres.paxos.impl.Instance;
import org.dancres.paxos.impl.LeaderSelection;

import java.net.InetSocketAddress;

public class OldRound implements PaxosMessage, LeaderSelection {
    private final long _seqNum;
    private final long _lastRound;
    private final InetSocketAddress _leaderNodeId;
    
    public OldRound(long aSeqNum, InetSocketAddress aLeaderNodeId, long aLastRound) {
        _seqNum = aSeqNum;
        _leaderNodeId = aLeaderNodeId;
        _lastRound = aLastRound;
    }

    public int getType() {
        return Operations.OLDROUND;
    }

    public short getClassification() {
    	return ACCEPTOR_LEARNER;
    }

    /**
     * Sequence number is always the last one completed (the AL low watermark).
     */
    public long getSeqNum() {
        return _seqNum;
    }

    public InetSocketAddress getLeaderNodeId() {
        return _leaderNodeId;
    }

    public long getLastRound() {
        return _lastRound;
    }

    /**
     * OldRound always indicates a leader is not in charge regardless of sequence number, thus it needn't be attached to a
     * specific sequence number and thus OldRound can contain most recent successful sequence number not the original
     */
    public boolean routeable(Instance anInstance) {
        return ((_lastRound >= anInstance.getRound()) &&
                ((anInstance.getState().equals(Instance.State.BEGIN)) ||
                 (anInstance.getState().equals(Instance.State.SUCCESS))));
    }

    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_lastRound).hashCode() ^ _leaderNodeId.hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof OldRound) {
    		OldRound myOther = (OldRound) anObject;
    		
    		return (myOther._seqNum == _seqNum) && (myOther._lastRound == _lastRound) && 
    			(myOther._leaderNodeId.equals(_leaderNodeId));
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "OldRound: " + Long.toHexString(_seqNum) + " [ " + Long.toHexString(_lastRound) + ", " +
                _leaderNodeId + " ]";
    }
}
