package org.dancres.paxos;

import org.dancres.paxos.impl.CheckpointHandle;

/**
 * An instance of Paxos is initialised as follows:
 *
 * <ol>
 * <li>It looks at the handle and uses that to identify the last seqnum contained in the snapshot (which would be
 * the low watermark at the time of snapshot).</li>
 * <li>It replays the log (if any) from snapshot.seq_num + 1 to end of the log, passing success values out to the
 * registered listener.</li>
 * <li>Leader activity can now commence and AL activity could trigger further recovery from log file or indeed
 * a later snapshot needing to be loaded. In the latter case, user code should be signalled "out of date" and given
 * an address from which to obtain a snapshot or similar. We could make the user code implement a method which can
 * invoked to get it's last snapshot which would then be passed to another AL to restore it. We might also arrange
 * for a <code>SnapshotHandle</code> to include a stream into which the user code writes it's snapshot after which it
 * can call close or similar which automatically completes the snapshot. This puts the snapshot under Paxos' control
 * and allows it to manage snapshot lifecycle at startup and when recovery is required.</li>
 * </ol>
 */
public interface Paxos {
    public interface Listener {
        public void done(Event anEvent);
    }

    public void close();
    public CheckpointHandle newCheckpoint();
    public void submit(byte[] aValue, byte[] aHandback);
    public void register(Listener aListener);
    public void bringUpToDate(CheckpointHandle aHandle) throws Exception;
}
