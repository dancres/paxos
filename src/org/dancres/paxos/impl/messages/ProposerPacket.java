package org.dancres.paxos.impl.messages;

public interface ProposerPacket extends PaxosMessage {
    public int getPort();
    public PaxosMessage getOperation();
}
