package org.dancres.paxos.impl.core;

public interface Reasons {
    public static int OTHER_LEADER = -1;
    public static int VOTE_TIMEOUT = -2;
    public static int BAD_MEMBERSHIP = -3;
}
