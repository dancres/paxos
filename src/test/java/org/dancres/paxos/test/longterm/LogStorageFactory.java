package org.dancres.paxos.test.longterm;

import org.dancres.paxos.LogStorage;

interface LogStorageFactory {
    LogStorage getLogger();
}
