package org.dancres.paxos.messages;

import java.net.InetSocketAddress;

public class Ack implements PaxosMessage {
    private long _seqNum;
    private InetSocketAddress _nodeId;
    
    public Ack(long aSeqNum, InetSocketAddress aNodeId) {
        _seqNum = aSeqNum;
        _nodeId = aNodeId;
    }

    public InetSocketAddress getNodeId() {
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
		return new Long(_seqNum).hashCode() ^ _nodeId.hashCode();
	}
	
	public boolean equals(Object anObject) {
		if (anObject instanceof Ack) {
			Ack myOther = (Ack) anObject;
			
			return (_seqNum == myOther._seqNum) && (_nodeId.equals(myOther._nodeId));
		}
		
		return false;
	}
	
    public String toString() {
        return "Ack: " + _seqNum;
    }
}
