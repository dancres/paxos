package org.dancres.paxos.impl.faildet;

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
