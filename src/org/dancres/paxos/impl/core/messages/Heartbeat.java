package org.dancres.paxos.impl.core.messages;

import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

public class Heartbeat implements PaxosMessage {
    public Heartbeat() {
    }
    
    public int getType() {
        return Operations.HEARTBEAT;
    }

    public long getSeqNum() {
        throw new RuntimeException("No sequence number on a heartbeat");
    }

    public String toString() {
        return "Hbeat";
    }
}
