package org.dancres.paxos.messages;

public interface PaxosMessage {
    public int getType();
    public long getSeqNum();
}
