package org.dancres.paxos.impl;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.impl.faildet.Heartbeater;

public interface MessageBasedFailureDetector extends FailureDetector {
    public void processMessage(Packet aPacket) throws Exception;
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

    public Heartbeater newHeartbeater(Transport aTransport, byte[] aMetaData);

    public void stop();
}
