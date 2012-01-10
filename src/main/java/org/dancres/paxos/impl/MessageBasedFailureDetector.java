package org.dancres.paxos.impl;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.messages.PaxosMessage;

public interface MessageBasedFailureDetector extends FailureDetector {
    public void processMessage(PaxosMessage aMessage) throws Exception;
    public Membership getMembers(MembershipListener aListener);

    /**
     * Currently a simple majority test - ultimately we only need one member of the previous majority to be present
     * in this majority for Paxos to work.
     *
     * @return true if at this point, available membership would allow for a majority
     */
    public boolean couldComplete();

    /**
     * @return the size of membership required for a majority
     */
    public int getMajority();

    public void stop();
}
