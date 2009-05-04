package org.dancres.paxos.impl.core;

public interface LogStorage {
    public byte[] get(long aSeqNum);
    public void put(long aSeqNum, byte[] aValue);
}
