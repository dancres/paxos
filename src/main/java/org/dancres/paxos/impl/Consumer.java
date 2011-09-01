package org.dancres.paxos.impl;

import org.dancres.paxos.messages.PaxosMessage;

public interface Consumer {
    public void process(PaxosMessage aMsg);
}
