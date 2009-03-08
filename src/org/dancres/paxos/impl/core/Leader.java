package org.dancres.paxos.impl.core;

public interface Leader {
    public static final int COLLECT = 0;
    public static final int BEGIN = 1;
    public static final int SUCCESS = 2;
    public static final int EXIT = 3;
    public static final int ABORT = 4;

    public long getSeqNum();
    public int getState();
}
