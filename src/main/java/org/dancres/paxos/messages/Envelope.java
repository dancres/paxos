package org.dancres.paxos.messages;

import java.net.InetSocketAddress;

import org.dancres.paxos.Proposal;

public class Envelope implements PaxosMessage {
    private long _seqNum = -1;
    private final Proposal _proposal;
    
    public Envelope(Proposal aValue) {
        _proposal = aValue;
    }
    
    public Envelope(long aSeqNum, Proposal aValue) {
        _seqNum = aSeqNum;
        _proposal = aValue;
    }

    public long getSeqNum() {
    	return _seqNum;
    }

    public int getType() {
        return Operations.ENVELOPE;
    }

    public short getClassification() {
    	return CLIENT;
    }
     
    public String toString() {
        return "Post";
    }

    public Proposal getValue() {
        return _proposal;
    }
}
