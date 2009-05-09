package org.dancres.paxos.impl.util;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import org.dancres.paxos.impl.core.LogStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryLogStorage implements LogStorage {
    private Logger _logger = LoggerFactory.getLogger(MemoryLogStorage.class);

    private ConcurrentHashMap<Long, byte[]> _log = new ConcurrentHashMap<Long, byte[]>();

    public byte[] get(long aSeqNum) {
        if (aSeqNum == LogStorage.EMPTY_LOG)
            return new byte[0];
        else
            return _log.get(new Long(aSeqNum));
    }

    public void put(long aSeqNum, byte[] aValue) {
        _logger.info("Storing: " + aSeqNum + " = " + Arrays.toString(aValue));

        if (aSeqNum < 0)
            throw new IllegalArgumentException("Sequence number must be non-negative");

        _log.put(new Long(aSeqNum), aValue);
    }
}
