package org.dancres.paxos;

import org.dancres.paxos.impl.CheckpointHandle;

import java.net.InetSocketAddress;

public interface Paxos {
    public interface Listener {
        public void done(Event anEvent);
    }

    public void close();
    public CheckpointHandle newCheckpoint();
    public void submit(Proposal aValue);
    public void register(Listener aListener);
    public void bringUpToDate(CheckpointHandle aHandle) throws Exception;
    public byte[] getMetaData(InetSocketAddress anAddress);
}
