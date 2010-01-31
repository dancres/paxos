package org.dancres.paxos.messages;

import org.dancres.paxos.ConsolidatedValue;

public class Post implements PaxosMessage {
    public static final int TYPE = Operations.POST;

    private byte[] _value;
    private byte[] _handback;
    private long _nodeId;
    
    public Post(byte[] aValue, byte[] aHandback, long aNodeId) {
        _value = aValue;
        _handback = aHandback;
        _nodeId = aNodeId;
    }
    
    public Post(ConsolidatedValue aValue, long aNodeId) {
    	_value = aValue.getValue();
    	_handback = aValue.getHandback();
    	_nodeId = aNodeId;
    }
    
    public long getSeqNum() {
        throw new RuntimeException("No sequence number on a post - initiates the state machine - not core protocol");
    }

    public int getType() {
        return Operations.POST;
    }

    public short getClassification() {
    	return CLIENT;
    }
     
    public long getNodeId() {
    	return _nodeId;
    }
    
    public String toString() {
        return "Post";
    }

    public byte[] getValue() {
        return _value;
    }

    public byte[] getHandback() {
        return _handback;
    }

    public ConsolidatedValue getConsolidatedValue() {
    	return new ConsolidatedValue(_value, _handback);
    }
}
