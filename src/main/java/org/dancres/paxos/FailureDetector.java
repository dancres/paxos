package org.dancres.paxos;

import org.dancres.paxos.impl.Membership;
import org.dancres.paxos.impl.MembershipListener;
import org.dancres.paxos.messages.PaxosMessage;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

/**
 * Base interface for failure detector implementations.  For more on failure detectors read: Unreliable Failure Detectors for
 * Reliable Distributed Systems by Tushar Deepak Chandra and Sam Toueg.
 *
 * @author dan
 */
public interface FailureDetector {
    public interface MetaData {
        public byte[] getData();
        public long getTimestamp();
    }

    public Map<InetSocketAddress, MetaData> getMemberMap();

    /**
     * Return a random member that the FD believes is live, excluding the local address specified
     *
     * @param aLocal the address of the node to exclude from the result
     */
    public InetSocketAddress getRandomMember(InetSocketAddress aLocal);
}