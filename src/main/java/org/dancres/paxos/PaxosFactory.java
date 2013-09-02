package org.dancres.paxos;

import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.storage.MemoryLogStorage;

public class PaxosFactory {
    public static Paxos init(int aClusterSize,
                             Listener aListener, CheckpointHandle aHandle, byte[] aMetaData) throws Exception {
        Core myCore = new Core(new MemoryLogStorage(), aHandle, aListener);
        Transport myTransport = new TransportImpl(new FailureDetectorImpl(aClusterSize, 5000), aMetaData);
        myTransport.routeTo(myCore);
        myCore.init(myTransport);

        return myCore;
    }

    public static Paxos init(int aClusterSize, Listener aListener, CheckpointHandle aHandle, byte[] aMetaData,
                             LogStorage aLogger) throws Exception {
        Core myCore = new Core(aLogger, aHandle, aListener);
        Transport myTransport = new TransportImpl(new FailureDetectorImpl(aClusterSize, 5000), aMetaData);
        myTransport.routeTo(myCore);
        myCore.init(myTransport);

        return myCore;
    }
}
