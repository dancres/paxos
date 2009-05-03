package org.dancres.paxos.impl.core;

public interface LogStorage {
    public byte[] get(long _seqNum);
    public void put(long _seqNum, byte[] value);
}
