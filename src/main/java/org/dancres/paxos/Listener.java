package org.dancres.paxos;

public interface Listener {
    public void transition(StateEvent anEvent);
}
