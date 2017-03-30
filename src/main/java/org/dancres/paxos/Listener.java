package org.dancres.paxos;

public interface Listener {
    void transition(StateEvent anEvent);
}
