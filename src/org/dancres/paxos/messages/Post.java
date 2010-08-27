package org.dancres.paxos.messages;

public class Post implements PaxosMessage {
    public static final int TYPE = Operations.POST;

    private byte[] _value;
    private long _nodeId;
    
    public Post(byte[] aValue, long aNodeId) {
        _value = aValue;
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
}
