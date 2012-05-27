package org.dancres.paxos;

import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.storage.MemoryLogStorage;

/**
 * @author dan
 */
public class PaxosFactory {
    public static Paxos init(Paxos.Listener aListener, CheckpointHandle aHandle, byte[] aMetaData) throws Exception {
        Core myCore = new Core(new FailureDetectorImpl(5000), new MemoryLogStorage(), aMetaData, aHandle, aListener);
        new TransportImpl().add(myCore);

        return myCore;
    }

    public static Paxos init(Paxos.Listener aListener, CheckpointHandle aHandle, byte[] aMetaData,
                             LogStorage aLogger) throws Exception {
        Core myCore = new Core(new FailureDetectorImpl(5000), aLogger, aMetaData, aHandle, aListener);
        new TransportImpl().add(myCore);

        return myCore;
    }
}
