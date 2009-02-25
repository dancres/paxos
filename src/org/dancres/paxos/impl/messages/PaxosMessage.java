package org.dancres.paxos.impl.messages;

public interface PaxosMessage {
    public int getType();
    public long getSeqNum();
}
