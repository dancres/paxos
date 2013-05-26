package org.dancres.paxos;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Represents a membership snapshot from a particular point in time.
 * This majority should be used until a Paxos round is completed or restarted.
 */
public interface Membership {
    /**
     * @return the current size of the membership
     */
    public int getSize();

    public boolean couldComplete();

    public boolean isMajority(Collection<InetSocketAddress> aListOfAddresses);
}
