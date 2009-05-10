package org.dancres.paxos.impl.core;

public interface LeaderListener {
    /**
     * Invoked by the leader when it's ready to accept more client work
     */
    public void ready();
}
