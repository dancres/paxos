package org.dancres.paxos.impl;

import org.dancres.paxos.FailureDetector;
import org.dancres.paxos.Membership;
import org.dancres.paxos.MembershipListener;
import org.dancres.paxos.impl.Transport.Packet;

public abstract class MessageBasedFailureDetector implements FailureDetector, MessageProcessor {
    public abstract Heartbeater newHeartbeater(Transport aTransport, byte[] aMetaData);

    public abstract void stop();
}
