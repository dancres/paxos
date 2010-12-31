package org.dancres.paxos;

import java.net.InetSocketAddress;

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
     * 
     * @return <code>true</code> if membership is sufficient for a vote, <code>false</code> otherwise.
     */
    public boolean startInteraction();

    /**
     * Leader of a round invokes this for each response received. As each node is expected to return a single message, any additions
     * are duplicates or not expected and thus should be discarded.
     * 
     * @return <code>true</code> if this response was expected, <code>false</code> otherwise.
     */
    public boolean receivedResponse(InetSocketAddress aNodeId);

    /**
     * Indicate this membership will be used no more
     */
    public void dispose();
}
