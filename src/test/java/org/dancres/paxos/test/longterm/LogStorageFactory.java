package org.dancres.paxos.test.longterm;

import org.dancres.paxos.LogStorage;

public interface LogStorageFactory {
    public LogStorage getLogger();
}
