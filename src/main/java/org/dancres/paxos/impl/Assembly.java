package org.dancres.paxos.impl;

import org.dancres.paxos.Membership;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Represents a membership snapshot from a particular point in time.
 * This majority should be used until a Paxos round is completed or restarted.
 */
public interface Assembly extends Membership {
    /**
     * @return the current size of the membership
     */
    int getSize();

    boolean couldComplete();

    boolean isMajority(Collection<InetSocketAddress> aListOfAddresses);
}
