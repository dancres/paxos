package org.dancres.paxos.messages;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.LogStorage;

public class Last implements PaxosMessage {
    private long _seqNum;
    private long _low;
    private long _rndNumber;
    private long _nodeId;
    private ConsolidatedValue _value;

    /**
     * @param aSeqNum is the sequence number received in the related collect
     * @param aLowWatermark is the last contiguous sequence completed
     * @param aMostRecentRound is the most recent leader round seen
     * @param aValue is the value, if any, associated with the sequence number of the related collect
     */
    public Last(long aSeqNum, long aLowWatermark, long aMostRecentRound, ConsolidatedValue aValue, long aNodeId) {
        _seqNum = aSeqNum;
        _low = aLowWatermark;
        _rndNumber = aMostRecentRound;
        _value = aValue;
        _nodeId = aNodeId;
    }

    public int getType() {
        return Operations.LAST;
    }

    public short getClassification() {
    	return ACCEPTOR_LEARNER;
    }
    
    /**
     * @return the value associated with the sequence number returned by <code>getSeqNum</code>
     */
    public ConsolidatedValue getConsolidatedValue() {
        return _value;
    }

    public long getNodeId() {
    	return _nodeId;
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
    		new Long(_rndNumber).hashCode() ^ new Long(_nodeId).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Last) {
    		Last myOther = (Last) anObject;
    		
    		return (_seqNum == myOther._seqNum) && (_low == myOther._low) && (_rndNumber == myOther._rndNumber) &&
    			(_nodeId == myOther._nodeId);
    	}
    	
    	return false;
    }
    
    public String toString() {
        return "Last: " + Long.toHexString(_seqNum) + " " + Long.toHexString(_low) + 
                " [ " + Long.toHexString(_rndNumber) + " ] " + _value.equals(LogStorage.NO_VALUE);
    }
}
