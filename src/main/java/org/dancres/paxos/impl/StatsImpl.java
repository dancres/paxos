package org.dancres.paxos.impl;

import java.util.concurrent.atomic.AtomicLong;

public class StatsImpl implements  Stats {
    /**
     * Statistic that tracks the number of Collects this AcceptorLearner ignored
     * from competing leaders within DEFAULT_LEASE ms of activity from the
     * current leader.
     */
    private final AtomicLong _ignoredCollects = new AtomicLong();

    /**
     * Statistic that tracks the number of leader heartbeats received.
     */
    private final AtomicLong _receivedHeartbeats = new AtomicLong();

    /**
     * Statistic that tracks the number of recoveries executed for this instance.
     */
    private final AtomicLong _recoveryCycles = new AtomicLong();

    /**
     * Statistic that tracks the number of active (non-recovery and as a member) votes for this instance.
     */
    private final AtomicLong _activeAccepts = new AtomicLong();

    public long getHeartbeatCount() {
        return _receivedHeartbeats.longValue();
    }

    void incrementHeartbeats() {
        _receivedHeartbeats.incrementAndGet();
    }

    public long getIgnoredCollectsCount() {
        return _ignoredCollects.longValue();
    }

    void incrementCollects() {
        _ignoredCollects.incrementAndGet();
    }

    public long getActiveAccepts() {
        return _activeAccepts.longValue();
    }

    void incrementAccepts() {
        _activeAccepts.incrementAndGet();
    }

    public long getRecoveryCycles() {
        return _recoveryCycles.longValue();
    }

    void incrementRecoveries() {
        _recoveryCycles.incrementAndGet();
    }
}
