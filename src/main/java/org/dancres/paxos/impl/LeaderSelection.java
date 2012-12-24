package org.dancres.paxos.impl;

public interface LeaderSelection {
    /**
     * A message that declares itself routable for a Leader should be passed to it for processing.
     *
     * @return Whether or not the implementor should be applied to this Paxos instance.
     */
    public boolean routeable(Leader aLeader);
}
