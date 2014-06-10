package org.dancres.paxos;

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

    public interface StateListener {
        public void change(FailureDetector aDetector, State aState);
    }

    public interface MetaData {
        public byte[] getData();
        public long getTimestamp();
    }

    /**
     * @param aLocal the address of the node to exclude from the result
     *
     * @return a random member that the FD believes is live, excluding the local address specified or <code>null</code>
     * if there are no suitable candidates.
     */
    public InetSocketAddress getRandomMember(InetSocketAddress aLocal);

    public boolean isMember(InetSocketAddress anAddress);

    public Membership getMembers();

    /**
     * @param anAddress is the Paxos node leader address
     * @return the userdata associated with the passed address, if it is known (FD membership is volatile and an entry
     * may be lost between getting the Paxos node leader address and the request for associated data).
     */
    public byte[] dataForNode(InetSocketAddress anAddress);

    /**
     * @return the size of membership required for a majority
     */
    public int getMajority();

    /**
     * @return A <code>Future</code> that will return a <code>Membership</code> snapshot when the
     * majority is achieved.
     */
    public Future<Membership> barrier();

    /**
     * @param aRequired is the number of members required
     * @return A <code>Future</code> that will return a <code>Membership</code> snapshot when the
     * specified size is achieved.
     */
    public Future<Membership> barrier(int aRequired);

    /**
     * Restrict membership to the specified set of members
     *
     * @param aMembers which, if <code>null</code>, causes the FailureDetector to become unpinned. When unpinned,
     *                 no membership will be tracked/maintained and Paxos will not make progress. Note that a pinned
     *                 membership is required to guarantee integrity although in some situations that constraint
     *                 might be relaxed.
     */
    public void pin(Collection<InetSocketAddress> aMembers);

    public void addListener(StateListener aListener);
}
