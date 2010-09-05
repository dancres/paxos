package org.dancres.paxos;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Base interface for failure detector implementations.  For more on failure detectors read: Unreliable Failure Detectors for
 * Reliable Distributed Systems by Tushar Deepak Chandra and Sam Toueg.
 *
 * @author dan
 */
public interface FailureDetector {
    public void add(LivenessListener aListener);
    public long getUnresponsivenessThreshold();
    public Membership getMembers(MembershipListener aListener);
    public Set<InetSocketAddress> getMemberSet();
    public byte[] getMetaData(InetSocketAddress aNode);    
    public boolean isLive(InetSocketAddress aNodeId);
    
    /**
     * Currently a simple majority test - ultimately we only need one member of the previous majority to be present
     * in this majority for Paxos to work.
     * 
     * @return true if at this point, available membership would allow for a majority
     */
    public boolean couldComplete();
    
    public void stop();    
}
