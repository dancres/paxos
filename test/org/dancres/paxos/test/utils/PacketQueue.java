package org.dancres.paxos.test.utils;

import org.dancres.paxos.messages.PaxosMessage;

public interface PacketQueue {
    public void add(PaxosMessage aMessage);
    public PaxosMessage getNext(long aPause) throws InterruptedException;
}
