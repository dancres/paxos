package org.dancres.paxos.test.utils;

public interface PacketQueue {
    public void add(Packet aPacket);
    public Packet getNext(long aPause) throws InterruptedException;
}
