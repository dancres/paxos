package org.dancres.paxos.impl;

public abstract class Constants {
	private static final long DEFAULT_LEADER_LEASE = 30000;
	
    public static final long NO_ROUND = -1;
    public static final long UNKNOWN_SEQ = -1;
    private static volatile long _leaderLease = DEFAULT_LEADER_LEASE;

    public enum FSMStates {INITIAL, ACTIVE, RECOVERING, OUT_OF_DATE, SHUTDOWN}

    public static void setLeaderLeaseDuration(long aDuration) {
        _leaderLease = aDuration;
    }

    public static long getLeaderLeaseDuration() {
    	return _leaderLease;
    }
}
