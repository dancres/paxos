package org.dancres.paxos.impl.core;

/**
 * Implement this interface to receive signals from the acceptor learner about current state.
 *
 * @author dan
 */
public interface AcceptorLearnerListener {
    public void done(Completion aCompletion);
}
