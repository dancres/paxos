package org.dancres.paxos.impl.core;

/**
 * Possible status reports returned to the client as part of a <code>Fail</code> message.
 *
 * @author dan
 */
public interface Reasons {
    public static int OTHER_LEADER = -1;
    public static int VOTE_TIMEOUT = -2;
    public static int BAD_MEMBERSHIP = -3;
}
