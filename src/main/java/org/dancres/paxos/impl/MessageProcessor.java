package org.dancres.paxos.impl;

public interface MessageProcessor {
    boolean accepts(Transport.Packet aPacket);
    void processMessage(Transport.Packet aPacket);
}
