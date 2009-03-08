package org.dancres.paxos.impl.core;

public interface LeaderListener {
    public void newState(Leader aLeader);
}
