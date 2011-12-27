package org.dancres.paxos.impl;

public interface LeaderSelection {
    /**
     * A message that declares itself routable for a Leader should be passed to it for processing.
     *
     * @param aLeader
     * @return
     */
    public boolean routeable(Leader aLeader);
}
