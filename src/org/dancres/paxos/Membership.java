package org.dancres.paxos;

import org.dancres.paxos.Address;

/**
 * Represents a membership snapshot from a particular point in time which will be updated
 * by the failure detector on the fly.  This majority should be used until a Paxos round is completed
 * or restarted.
 */
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
    public void receivedResponse(Address anAddress);

    /**
     * Indicate this membership will be used no more
     */
    public void dispose();

    /**
     * @return the size of membership required for a majority
     */
    public int getMajority();
}
