package org.dancres.paxos;

public interface Listener {
    public void done(StateEvent anEvent);
}
