package org.dancres.paxos.messages;

public class Ack implements PaxosMessage {
    private long _seqNum;

    public Ack(long aSeqNum) {
        _seqNum = aSeqNum;
    }

    public int getType() {
        return Operations.ACK;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public String toString() {
        return "Ack: " + _seqNum;
    }
}
