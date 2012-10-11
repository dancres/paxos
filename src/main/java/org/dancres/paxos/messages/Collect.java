package org.dancres.paxos.messages;

import org.dancres.paxos.impl.Constants;
import org.dancres.paxos.impl.LeaderUtils;

import java.net.InetSocketAddress;

public class Collect implements PaxosMessage {
    private long _seqNum;
    private long _rndNumber;
    private InetSocketAddress _nodeId;

    public static final Collect INITIAL = new Collect(Constants.UNKNOWN_SEQ, Long.MIN_VALUE);

    private Collect(long aSeqNum, long aRndNumber) {
        this(aSeqNum, aRndNumber, new InetSocketAddress(0));
    }

    public Collect(long aSeqNum, long aRndNumber, InetSocketAddress aNodeId) {
        _seqNum = aSeqNum;
        _rndNumber = aRndNumber;
        _nodeId = aNodeId;
    }

    public int getType() {
        return Operations.COLLECT;
    }
    
    public short getClassification() {
    	return LEADER;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public long getRndNumber() {
        return _rndNumber;
    }

    public InetSocketAddress getNodeId() {
        return _nodeId;
    }

    public String toString() {
        return "Collect: " + Long.toHexString(_seqNum) + " [ " +
                Long.toHexString(_rndNumber) + ", " + _nodeId + " ] ";
    }

    public boolean isInitial() {
    	return this.equals(INITIAL);
    }
    
    public int hashCode() {
    	return _nodeId.hashCode() ^ new Long(_rndNumber).hashCode() ^ new Long(_seqNum).hashCode();
    }
    
    public boolean equals(Object anObject) {
    	if (anObject instanceof Collect) {
    		Collect myOther = (Collect) anObject;
    		
    		return ((myOther._nodeId.equals(_nodeId)) && (myOther._rndNumber == _rndNumber) && 
    				(myOther._seqNum == _seqNum)); 
    	}
    	
    	return false;
    }
}
