package org.dancres.paxos.impl;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.Future;

/**
 * Base interface for failure detector implementations.  For more on failure detectors read: Unreliable Failure
 * Detectors for Reliable Distributed Systems by Tushar Deepak Chandra and Sam Toueg.
 *
 * @author dan
 */
public interface FailureDetector {
    enum State {PINNED, OPEN, STOPPED}

    interface StateListener {
        void change(FailureDetector aDetector, State aState);
    }

    interface MetaData {
        byte[] getData();
        long getTimestamp();
    }

    /**
     * @param aLocal the address of the node to exclude from the result
     *
     * @return a random member that the FD believes is live, excluding the local address specified or <code>null</code>
     * if there are no suitable candidates.
     */
    InetSocketAddress getRandomMember(InetSocketAddress aLocal);

    boolean isMember(InetSocketAddress anAddress);

    Assembly getMembers();

    /**
     * @param anAddress is the Paxos node leader address
     * @return the userdata associated with the passed address, if it is known (FD membership is volatile and an entry
     * may be lost between getting the Paxos node leader address and the request for associated data).
     */
    byte[] dataForNode(InetSocketAddress anAddress);

    /**
     * @return the size of membership required for a majority
     */
    int getMajority();

    /**
     * @return A <code>Future</code> that will return a <code>Membership</code> snapshot when the
     * majority is achieved.
     */
    Future<Assembly> barrier();

    /**
     * @param aRequired is the number of members required
     * @return A <code>Future</code> that will return a <code>Membership</code> snapshot when the
     * specified size is achieved.
     */
    Future<Assembly> barrier(int aRequired);

    /**
     * Restrict membership to the specified set of members
     *
     * @param aMembers which, if <code>null</code>, causes the FailureDetector to become unpinned. When unpinned,
     *                 no membership will be tracked/maintained and Paxos will not make progress. Note that a pinned
     *                 membership is required to guarantee integrity although in some situations that constraint
     *                 might be relaxed.
     */
    void pin(Collection<InetSocketAddress> aMembers);

    void addListener(StateListener aListener);
}
