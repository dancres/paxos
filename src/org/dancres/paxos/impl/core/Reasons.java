package org.dancres.paxos.impl.core;

/**
 * Possible status reports returned to the client as part of a <code>Fail</code> message.
 *
 * @author dan
 */
public interface Reasons {
    public static final int OK = 0;
    public static final int OTHER_LEADER = -1;
    public static final int VOTE_TIMEOUT = -2;
    public static final int BAD_MEMBERSHIP = -3;
}
