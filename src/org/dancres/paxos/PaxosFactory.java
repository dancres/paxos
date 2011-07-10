package org.dancres.paxos;

import org.dancres.paxos.impl.CheckpointHandle;

public class PaxosFactory {
    /**
     * An application should first load it's last available snapshot which should include an instance of a
     * SnapshotHandle which should be passed to <code>init</code>. If the application has no previous snapshot
     * and no thus no valid <code>SnapshotHandle</code> it should pass SnapshotHandle.NO_SNAPSHOT
     *
     * @param aListener that will receive agreed messages, snapshots etc.
     * @param aHandle is the handle last given to the application as result of a snapshot.
     */
    public static Paxos init(Paxos.Listener aListener, CheckpointHandle aHandle) {
        throw new UnsupportedOperationException();
    }

    public static Paxos init(Paxos.Listener aListener, CheckpointHandle aHandle, byte[] aMetaData) {
        throw new UnsupportedOperationException();
    }
}
