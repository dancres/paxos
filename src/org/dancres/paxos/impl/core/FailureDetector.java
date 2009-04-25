package org.dancres.paxos.impl.core;

public interface FailureDetector {
    public long getUnresponsivenessThreshold();
    public Membership getMembers(MembershipListener aListener);
}
