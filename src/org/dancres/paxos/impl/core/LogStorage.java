package org.dancres.paxos.impl.core;

/**
 * Standard abstraction for the log required to maintain essential paxos state to ensure appropriate recovery.
 *
 * @author dan
 */
public interface LogStorage {
    public static final long EMPTY_LOG = Long.MIN_VALUE;
    public static final byte[] NO_VALUE = new byte[0];

    public byte[] get(long aSeqNum);
    public void put(long aSeqNum, byte[] aValue);
}
