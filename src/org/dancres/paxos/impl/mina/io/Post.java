package org.dancres.paxos.impl.mina.io;

import org.dancres.paxos.impl.core.messages.*;

/**
 * Post is passed across transports as a client request but is not a valid message for the leader state machine.  It is thus an instance of
 * PaxosMessage to keep the comms stack clean and simple but cannot be considered a state machine message.
 * 
 * @author dan
 */
public class Post implements PaxosMessage {
    public static final int TYPE = Operations.POST;

    private byte[] _value;

    public Post(byte[] aValue) {
        _value = aValue;
    }
    
    public long getSeqNum() {
        throw new RuntimeException("No sequence number on a post - you didn't submit this to the state machine did you?");
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
