package org.dancres.paxos.impl.core;

/**
 * Base interface for failure detector implementations.  For more on failure detectors read: Unreliable Failure Detectors for
 * Reliable Distributed Systems by Tushar Deepak Chandra and Sam Toueg.
 *
 * @author dan
 */
public interface FailureDetector {
    public long getUnresponsivenessThreshold();
    public Membership getMembers(MembershipListener aListener);
}
