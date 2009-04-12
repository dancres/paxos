package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.PaxosMessage;

public interface Transport {
    public void send(PaxosMessage aMessage, Address anAddress);
}
