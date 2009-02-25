package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.messages.PaxosMessage;

public interface Channel {
    public void write(PaxosMessage aMessage);
}
