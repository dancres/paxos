package org.dancres.paxos;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Base interface for failure detector implementations.  For more on failure detectors read: Unreliable Failure
 * Detectors for Reliable Distributed Systems by Tushar Deepak Chandra and Sam Toueg.
 *
 * @author dan
 */
public interface FailureDetector {
    public interface MetaData {
        public byte[] getData();
        public long getTimestamp();
    }

    /**
     * Return a random member that the FD believes is live, excluding the local address specified
     *
     * @param aLocal the address of the node to exclude from the result
     */
    public InetSocketAddress getRandomMember(InetSocketAddress aLocal);

    public Membership getMembers();

    /**
     * @return the size of membership required for a majority
     */
    public int getMajority();

    public Future<Boolean> barrier();
}
