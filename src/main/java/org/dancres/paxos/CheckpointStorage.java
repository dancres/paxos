package org.dancres.paxos;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Used to manage a collection of checkpoints on behalf of an application. Typical challenges include retiring old
 * ones as new ones are generated and atomic generation of checkpoints.
 */
public interface CheckpointStorage {
    interface WriteCheckpoint {
        void saved();
        OutputStream getStream() throws IOException;
    }

    interface ReadCheckpoint {
        InputStream getStream() throws IOException;
    }

    int numFiles();
    ReadCheckpoint getLastCheckpoint();
    WriteCheckpoint newCheckpoint();
}
