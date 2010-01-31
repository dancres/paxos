package org.dancres.paxos.messages;

import org.dancres.paxos.NodeId;

public class Complete implements PaxosMessage {
    private long _seqNum;

    public Complete(long aSeqNum) {
        _seqNum = aSeqNum;
    }

    public int getType() {
        return Operations.COMPLETE;
    }

    public long getNodeId() {
    	return NodeId.BROADCAST.asLong();
    }
    
    public short getClassification() {
    	return CLIENT;
    }

   public long getSeqNum() {
        return _seqNum;
    }

    public String toString() {
        return "Complete: " + _seqNum;
    }
}
