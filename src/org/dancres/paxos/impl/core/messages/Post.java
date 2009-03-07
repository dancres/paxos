package org.dancres.paxos.impl.core.messages;

import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.messages.Operations;

public class Post implements PaxosMessage {
    private byte[] _value;

    public Post(byte[] aValue) {
        _value = aValue;
    }
    
    public long getSeqNum() {
        throw new RuntimeException("No sequence number on a post");
    }

    public int getType() {
        return Operations.POST;
    }

    public byte[] getData() {
        return _value;
    }

    public String toString() {
        return "Post";
    }

    public byte[] getValue() {
        return _value;
    }
}
