package org.dancres.paxos.messages;

public class Ack implements PaxosMessage {
    private long _seqNum;
    private long _nodeId;
    
    public Ack(long aSeqNum, long aNodeId) {
        _seqNum = aSeqNum;
        _nodeId = aNodeId;
    }

    public long getNodeId() {
    	return _nodeId;
    }

    public int getType() {
        return Operations.ACK;
    }

    public short getClassification() {
    	return ACCEPTOR_LEARNER;
    }

	public long getSeqNum() {
		return _seqNum;
	}

	public int hashCode() {
		return new Long(_seqNum).hashCode() ^ new Long(_nodeId).hashCode();
	}
	
	public boolean equals(Object anObject) {
		if (anObject instanceof Ack) {
			Ack myOther = (Ack) anObject;
			
			return (_seqNum == myOther._seqNum) && (_nodeId == myOther._nodeId);
		}
		
		return false;
	}
	
    public String toString() {
        return "Ack: " + _seqNum;
    }
}
