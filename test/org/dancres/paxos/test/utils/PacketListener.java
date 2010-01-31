package org.dancres.paxos.test.utils;

import org.dancres.paxos.messages.PaxosMessage;

public interface PacketListener {
    public void deliver(PaxosMessage aMessage) throws Exception;
}
