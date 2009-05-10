package org.dancres.paxos.impl.core;

/**
 * Implement this interface to receive signals from the leader about current state.
 *
 * @author dan
 */
public interface LeaderListener {
    /**
     * Invoked by the leader when it's ready to accept more client work
     */
    public void ready();
}
