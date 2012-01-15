package org.dancres.paxos.impl;

import org.dancres.paxos.messages.PaxosMessage;

/**
 * Accepts log messages from a <code>Producer</code>.
 */
public interface Consumer {
    public void process(PaxosMessage aMsg, long aLogOffset);
}
