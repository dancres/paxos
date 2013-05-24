package org.dancres.paxos.impl;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.Membership;
import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.impl.Transport.Packet;

public interface MessageBasedFailureDetector extends FailureDetector {
    public void processMessage(Packet aPacket) throws Exception;

    public Heartbeater newHeartbeater(Transport aTransport, byte[] aMetaData);

    public void stop();
}
