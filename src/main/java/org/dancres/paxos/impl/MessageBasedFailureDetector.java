package org.dancres.paxos.impl;

public abstract class MessageBasedFailureDetector implements FailureDetector, MessageProcessor {
    public abstract Heartbeater newHeartbeater(Transport aTransport, byte[] aMetaData);

    public abstract void stop();
}
