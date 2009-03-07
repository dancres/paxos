package org.dancres.paxos.test.utils;

public interface PacketListener {
    public void deliver(Packet aPacket) throws Exception;
}
