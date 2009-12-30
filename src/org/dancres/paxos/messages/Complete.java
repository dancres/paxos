package org.dancres.paxos.messages;

public class Complete implements PaxosMessage {
    private long _seqNum;

    public Complete(long aSeqNum) {
        _seqNum = aSeqNum;
    }

    public int getType() {
        return Operations.COMPLETE;
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
