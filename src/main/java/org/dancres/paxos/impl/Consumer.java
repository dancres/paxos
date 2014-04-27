package org.dancres.paxos.impl;

/**
 * Accepts log messages from a <code>Producer</code>.
 */
interface Consumer {
    public void process(Transport.Packet aMsg, long aLogOffset);
}
