package org.dancres.paxos.impl;

public final class Constants {
    public static final long UNKNOWN_SEQ = -1;
    public static final int DEFAULT_MAX_INFLIGHT = 1;

    public enum EVENTS {
        PROP_ALLOC_INFLIGHT, PROP_ALLOC_ALL_CONCLUDED, LD_OUTCOME, AL_TRANSITION
    }
}
