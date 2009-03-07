package org.dancres.paxos.impl.core.messages;

public interface ProposerPacket extends PaxosMessage {
    public int getPort();
    public PaxosMessage getOperation();
}
