package org.dancres.paxos.messages;

import java.net.InetSocketAddress;

import org.dancres.paxos.Proposal;

public class Envelope implements PaxosMessage {
    public static final int TYPE = Operations.ENVELOPE;

    private long _seqNum = -1;
    private Proposal _proposal;
    private InetSocketAddress _nodeId;
    
    public Envelope(Proposal aValue, InetSocketAddress aNodeId) {
        _proposal = aValue;
        _nodeId = aNodeId;
    }
    
    public Envelope(long aSeqNum, Proposal aValue, InetSocketAddress aNodeId) {
        _seqNum = aSeqNum;
        _proposal = aValue;
        _nodeId = aNodeId;
    }

    public long getSeqNum() {
    	return _seqNum;
    }

    public int getType() {
        return TYPE;
    }

    public short getClassification() {
    	return CLIENT;
    }
     
    public InetSocketAddress getNodeId() {
    	return _nodeId;
    }
    
    public String toString() {
        return "Post";
    }

    public Proposal getValue() {
        return _proposal;
    }
}
