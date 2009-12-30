package org.dancres.paxos.messages;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.*;

public class Post implements PaxosMessage {
    public static final int TYPE = Operations.POST;

    private byte[] _value;
    private byte[] _handback;
    
    public Post(byte[] aValue, byte[] aHandback) {
        _value = aValue;
        _handback = aHandback;
    }
    
    public Post(byte[] aConsolidatedValue) {
        ByteBuffer myBuffer = ByteBuffer.wrap(aConsolidatedValue);
        int myValueSize = myBuffer.getInt();

        _value = new byte[myValueSize];
        _handback = new byte[aConsolidatedValue.length - 4 - myValueSize];
        myBuffer.get(_value);
        myBuffer.get(_handback);    	
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
        
    public String toString() {
        return "Post";
    }

    public byte[] getValue() {
        return _value;
    }

    public byte[] getHandback() {
        return _handback;
    }

    public byte[] getConsolidatedValue() {
        ByteBuffer myBuffer = ByteBuffer.allocate(4 + _handback.length + _value.length);
        myBuffer.putInt(_value.length);
        myBuffer.put(_value);
        myBuffer.put(_handback);

        return myBuffer.array();
    }
}
