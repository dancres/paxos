package org.dancres.paxos.test.longterm;

import org.dancres.paxos.LogStorage;
import org.dancres.paxos.storage.MemoryLogStorage;

class MemoryLoggerFactory implements LogStorageFactory {
    private final MemoryLogStorage _logger = new MemoryLogStorage();

    public LogStorage getLogger() {
        return _logger;
    }
}
