package org.dancres.paxos;

/**
 * Implement this interface to receive signals from the acceptor learner about current state.
 *
 * @author dan
 */
public interface Listener {
    public void done(Event anEvent);
}
