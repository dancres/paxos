package org.dancres.paxos;

import org.dancres.paxos.impl.CheckpointHandle;

import java.net.InetSocketAddress;

public interface Paxos {
    public interface Listener {
        public void done(VoteOutcome anEvent);
    }

    public class InactiveException extends Exception {
    }

    public void close();
    public CheckpointHandle newCheckpoint();

    /**
     * @param aValue
     * @throws InactiveException if the Paxos instance is currently out of date and in need of a new checkpoint or
     * shutting down. Note that technically it would be an error to incur this exception. This is the library user
     * should either have requested the shutdown and thus avoid making this request or received an out of date
     * <code>VoteOutcome</code> and be in the process of obtaining a new checkpoint.
     */
    public void submit(Proposal aValue) throws InactiveException;
    public void register(Listener aListener);
    public void bringUpToDate(CheckpointHandle aHandle) throws Exception;
    public byte[] getMetaData(InetSocketAddress anAddress);
}
