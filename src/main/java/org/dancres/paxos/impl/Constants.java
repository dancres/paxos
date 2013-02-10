package org.dancres.paxos.impl;

public abstract class Constants {
	private static final long DEFAULT_LEADER_LEASE = 30000;
	
    public static final long NO_ROUND = -1;
    public static final long UNKNOWN_SEQ = -1;

    public enum FSMStates {INITIAL, ACTIVE, RECOVERING, OUT_OF_DATE, SHUTDOWN}

    public static long getLeaderLeaseDuration() {
    	return DEFAULT_LEADER_LEASE;
    }
}
