package org.dancres.paxos;

import org.dancres.paxos.impl.Core;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.impl.netty.TransportImpl;

public class PaxosFactory {
    /**
     * @param aClusterSize is the size of the cluster
     * @param aListener is the listener to pass <code>StateEvent</code>'s to
     * @param aHandle is the handle for the most recent checkpoint taken by the user-code
     * @param aClientId is the id for the user-code service instance request a <code>Paxos</code> instance.
     * @param aLogger is the logger implementation to use for the <code>Paxos</code> log. Typically this would be
     *                persistent.
     *
     * @throws Exception
     */
    public static Paxos init(int aClusterSize, Listener aListener, CheckpointHandle aHandle, byte[] aClientId,
                             LogStorage aLogger) throws Exception {
        Core myCore = new Core(aLogger, aHandle, aListener);
        Transport myTransport = new TransportImpl(new FailureDetectorImpl(aClusterSize, 5000), aClientId);
        myCore.init(myTransport);

        return myCore;
    }
}
