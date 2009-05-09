package org.dancres.paxos.impl.core;

public interface LogStorage {
    public static final long EMPTY_LOG = Long.MIN_VALUE;

    public byte[] get(long aSeqNum);
    public void put(long aSeqNum, byte[] aValue);
}
