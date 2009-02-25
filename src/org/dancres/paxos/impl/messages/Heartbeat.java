package org.dancres.paxos.impl.messages;

import org.dancres.paxos.impl.messages.Operations;
import org.dancres.paxos.impl.messages.PaxosMessage;

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
