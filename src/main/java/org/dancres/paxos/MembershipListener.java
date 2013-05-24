package org.dancres.paxos;

/**
 * Registered with a <code>FailureDetector</code> instance to track critical membership lifecycle events.
 *
 * @author dan
 */
public interface MembershipListener {
    /**
     * Invoked if the membership service determines that correct progress cannot be made
     */
    public void abort();

    /**
     * Invoked if the membership service determines that all responses for a given interaction have been received
     */
    public void allReceived();
}
