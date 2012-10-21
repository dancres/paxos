package org.dancres.paxos.messages;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.impl.Leader;
import org.dancres.paxos.impl.LeaderSelection;

import java.net.InetSocketAddress;

public class Last implements PaxosMessage, LeaderSelection {
    private long _seqNum;
    private long _low;
    private long _rndNumber;
    private Proposal _value;

    /**
     * @param aSeqNum is the sequence number received in the related collect
     * @param aLowWatermark is the last contiguous sequence completed
     * @param aMostRecentRound is the most recent leader round seen
     * @param aValue is the value, if any, associated with the sequence number of the related collect
     */
    public Last(long aSeqNum, long aLowWatermark, long aMostRecentRound, Proposal aValue) {
        _seqNum = aSeqNum;
        _low = aLowWatermark;
        _rndNumber = aMostRecentRound;
        _value = aValue;
    }

    public int getType() {
        return Operations.LAST;
    }

    public short getClassification() {
    	return ACCEPTOR_LEARNER;
    }

    public boolean routeable(Leader aLeader) {
        return ((_seqNum == aLeader.getSeqNum()) && (aLeader.getState().equals(Leader.States.BEGIN)));
    }

    /**
     * @return the value associated with the sequence number returned by <code>getSeqNum</code>
     */
    public Proposal getConsolidatedValue() {
        return _value;
    }

    /**
     * @return the most recent leader round seen
     */
    public long getRndNumber() {
        return _rndNumber;
    }

    /**
     * @return the last contiguous sequence number seen
     */
    public long getLowWatermark() {
        return _low;
    }

    /**
     * @return the sequence number received in the related collect
     */
    public long getSeqNum() {
        return _seqNum;
    }

    public int hashCode() {
    	return new Long(_seqNum).hashCode() ^ new Long(_low).hashCode() ^ 
    		new Long(_rndNumber).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Last) {
    		Last myOther = (Last) anObject;
    		
    		return (_seqNum == myOther._seqNum) && (_low == myOther._low) && (_rndNumber == myOther._rndNumber);
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Last: " + Long.toHexString(_seqNum) + " " + Long.toHexString(_low) + 
                " [ " + Long.toHexString(_rndNumber) + " ] " + _value.equals(Proposal.NO_VALUE);
    }
}
