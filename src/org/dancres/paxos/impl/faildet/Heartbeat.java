package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.impl.core.messages.PaxosMessage;

/**
 * Message produced by <code>HeartbeaterImpl</code> for consumption and processing by <code>FailureDetectorImpl</code>
 *
 * @author dan
 */
public class Heartbeat implements PaxosMessage {
    public static final int TYPE = 0;

    public Heartbeat() {
    }
    
    public int getType() {
        return TYPE;
    }

    public long getSeqNum() {
        throw new RuntimeException("No sequence number on a heartbeat");
    }

    public String toString() {
        return "Hbeat";
    }
}
