package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.messages.PaxosMessage;

/**
 * Message produced by <code>HeartbeaterImpl</code> for consumption and processing by <code>FailureDetectorImpl</code>
 *
 * @author dan
 */
public class Heartbeat implements PaxosMessage {
    public static final int TYPE = 0;

    private long _addr;
    
    public Heartbeat(long anId) {
    	_addr = anId;
    }
    
    public long getNodeId() {
    	return _addr;
    }
    
    public int getType() {
        return TYPE;
    }

    public short getClassification() {
    	return FAILURE_DETECTOR;
    }
        
    public long getSeqNum() {
        throw new RuntimeException("No sequence number on a heartbeat");
    }

    public String toString() {
        return "Hbeat";
    }
    
    public boolean isResponse() {
    	return false;
    }
}
