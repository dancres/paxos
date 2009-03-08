package org.dancres.paxos.impl.core;

public interface LeaderListenerFactory {
    public LeaderListener newListener();
}
