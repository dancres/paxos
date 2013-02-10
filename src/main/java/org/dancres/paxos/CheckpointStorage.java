package org.dancres.paxos;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Used to manage a collection of checkpoints on behalf of an application. Typical challenges include retiring old
 * ones as new ones are generated and atomic generation of checkpoints.
 */
public interface CheckpointStorage {
    public interface WriteCheckpoint {
        public void saved();
        public OutputStream getStream() throws IOException;
    }

    public interface ReadCheckpoint {
        public InputStream getStream() throws IOException;
    }

    public int numFiles();
    public ReadCheckpoint getLastCheckpoint();
    public WriteCheckpoint newCheckpoint();
}
