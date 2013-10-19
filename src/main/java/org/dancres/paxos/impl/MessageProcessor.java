package org.dancres.paxos.impl;

public interface MessageProcessor {
    public boolean accepts(Transport.Packet aPacket);
    public void processMessage(Transport.Packet aPacket);
}
