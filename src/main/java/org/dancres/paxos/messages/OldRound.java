package org.dancres.paxos.messages;

import org.dancres.paxos.impl.Instance;
import org.dancres.paxos.impl.LeaderSelection;

import java.net.InetSocketAddress;
import java.util.EnumSet;

/**
 * Used to report that a leader is out of date as compared to the rest of the cluster. This happens when another
 * leader has been active, whether or not it managed to complete an instance. It can also happen when an AL identifies
 * that the Leader is attempting to drive an outcome for a sequence number prior to the most recent checkpoint.
 *
 * In either case, the leader is expected to base it's next attempt to drive the cluster on the returned information
 * (sequence and round numbers).
 */
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
        return Types.OLDROUND;
    }

    public EnumSet<Classification> getClassifications() {
    	return EnumSet.of(Classification.LEADER);
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
     * specific sequence number and thus OldRound can contain most recent successful sequence number not the original.
     * Note that two leaders at initial cluster start could attempt to use the same base round and thus it is necessary
     * to test the _lastRound for = as well as &gt;.
     */
    public boolean routeable(Instance anInstance) {
        return ((_lastRound >= anInstance.getRound()) &&
                ((anInstance.getState().equals(Instance.State.BEGIN)) ||
                 (anInstance.getState().equals(Instance.State.VOTING))));
    }

    public int hashCode() {
    	return Long.valueOf(_seqNum).hashCode() ^ Long.valueOf(_lastRound).hashCode() ^ _leaderNodeId.hashCode();
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
