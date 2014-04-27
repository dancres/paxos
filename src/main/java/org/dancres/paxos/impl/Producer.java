package org.dancres.paxos.impl;

/**
 * Recovers and sends a sequence of <code>PaxosMessage</code> instances to a <code>Consumer</code>
 */
interface Producer {
    public void produce(long aLogOffset) throws Exception;
}
