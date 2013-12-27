package org.dancres.paxos.impl;

public final class Constants {
	private static final long DEFAULT_LEADER_LEASE = 30000;

    public static final long UNKNOWN_SEQ = -1;
    private static volatile long _leaderLease = DEFAULT_LEADER_LEASE;

    public static void setLeaderLeaseDuration(long aDuration) {
        _leaderLease = aDuration;
    }

    public static long getLeaderLeaseDuration() {
    	return _leaderLease;
    }
}
