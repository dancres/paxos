package org.dancres.paxos.impl;

import org.dancres.paxos.FailureDetector;

public abstract class MessageBasedFailureDetector implements FailureDetector, MessageProcessor {
    public abstract Heartbeater newHeartbeater(Transport aTransport, byte[] aMetaData);

    public abstract void stop();
}
