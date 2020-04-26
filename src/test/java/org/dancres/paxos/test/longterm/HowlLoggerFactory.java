package org.dancres.paxos.test.longterm;

import org.dancres.paxos.LogStorage;
import org.dancres.paxos.storage.HowlLogger;
import org.dancres.paxos.test.utils.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

class HowlLoggerFactory implements LogStorageFactory {
    private static final Logger _logger = LoggerFactory.getLogger(HowlLoggerFactory.class);

    private final String _dir;

    /**
     * TODO: Change initial directory cleanup to account for disk storage loss simulation
     */
    HowlLoggerFactory(String aBase, int aNodeId) {
        _dir = aBase + "node" + aNodeId + "logs";

        _logger.info("Cleaning log directory");
        FileSystem.deleteDirectory(new File(_dir));
    }

    public LogStorage getLogger() {
        return new HowlLogger(_dir);
    }

    public String toString() {
        return "Howl: " + _dir;
    }
}
