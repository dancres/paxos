package org.dancres.paxos.impl.faildet;

public interface Membership {
    /**
     * @return the current size of the membership
     */
    public int getSize();

    /**
     * Invoke this before starting a round of interaction for Paxos
     */
    public void startInteraction();

    /**
     * Invoke this for each response received (duplicates should be filtered before making this call)
     */
    public void receivedResponse();

    /**
     * Indicate this membership will be used no more
     */
    public void dispose();

    /**
     * @return the size of membership required for a majority
     */
    public int getMajority();
}
