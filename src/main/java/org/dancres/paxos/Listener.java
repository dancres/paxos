package org.dancres.paxos;

public interface Listener {
    void transition(StateEvent anEvent);

    /**
     * Use this only for testing - all other Paxos instances should have fully implemented listeners
     */
    Listener NULL_LISTENER = (StateEvent anEvent) -> {};
}
