package org.dancres.paxos.impl;

interface Stats {
    long getHeartbeatCount();
    long getIgnoredCollectsCount();
    long getActiveAccepts();
    long getRecoveryCycles();
}
