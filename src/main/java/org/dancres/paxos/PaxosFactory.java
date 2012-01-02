package org.dancres.paxos;

import org.dancres.paxos.impl.CheckpointHandle;
import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.LogStorage;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.impl.util.MemoryLogStorage;

/**
 * @todo Convert to use HowlLogger
 * 
 * @author dan
 *
 */
public class PaxosFactory {
    /**
     * An application should first load it's last available snapshot which should include an instance of a
     * SnapshotHandle which should be passed to <code>init</code>. If the application has no previous snapshot
     * and no thus no valid <code>SnapshotHandle</code> it should pass SnapshotHandle.NO_SNAPSHOT
     *
     * @param aListener that will receive agreed messages, snapshots etc.
     * @param aHandle is the handle last given to the application as result of a snapshot.
     */
    public static Paxos init(Paxos.Listener aListener, CheckpointHandle aHandle, byte[] aMetaData) throws Exception {
        Core myCore = new Core(5000, new MemoryLogStorage(), aMetaData, aHandle, aListener);
        new TransportImpl().add(myCore);

        return myCore;
    }

    public static Paxos init(Paxos.Listener aListener, CheckpointHandle aHandle, byte[] aMetaData,
                             LogStorage aLogger) throws Exception {
        Core myCore = new Core(5000, aLogger, aMetaData, aHandle, aListener);
        new TransportImpl().add(myCore);

        return myCore;
    }
}
