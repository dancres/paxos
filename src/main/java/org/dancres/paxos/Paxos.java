package org.dancres.paxos;

/**
 * Service implementation notes:
 *
 * <p>Leader selection is a client-based element (or at least outside of the library). It is up to the client to decide
 * which leader to go to and it may be re-directed should that leader be aware of another leader. A failing leader
 * provides no guidance in respect of other leadership. Note that the decision process for leader selection should
 * be based on some consistent ordering of leader addresses, random selection or similar will lead to constant
 * leader conflict and no progress.</p>
 *
 * <p>Paxos needs only to be used to make changes to state. Read operations need not be passed through Paxos but they
 * do need to be dispatched to a node that is up-to-date. The node that is pretty much guaranteed to be up to date
 * is the existing leader. Other nodes could be used to support read operations so long as a level of staleness is
 * acceptable. If the state is timestamped in some fashion, once can use those timestamps to ensure that updates
 * based on the state are applied to the latest version (by checking its timestamp) or rejected.</p>
 *
 * <p>A server using this library needs to handle <code>VoteOutcome.Reason.OUT_OF_DATE</code>. It also needs to handle
 * <code>VoteOutcome.Reason.OTHER_LEADER</code>. In both cases it typically passes a message to it's client to request
 * a switch of leader. One means of doing this would be as follows:</p>
 *
 * <p>Use the meta data per Paxos node to hold the server contact details (ip, port etc). In response to
 * <code>OTHER_LEADER</code> it would pull the relevant meta data (via <code>getMetaData</code> and re-direct the
 * client. In the case of <code>OUT_OF_DATE</code>, it would use some "well known" policy to determine an alternate
 * leader, pull the meta data and re-direct the client.</p>
 *
 * <p>All other types of error should dispatch a fail to the client for the request it submitted. Should a request
 * succeed, it is server dependent what action is taken. It could be to execute a command or change a value etc.</p>
 *
 * <p>Whilst the core library has some automatic recovery mechanisms it is possible for a replica state machine to get
 * too far out of date. In these cases, <code>OUT_OF_DATE</code> is generated and the library client should
 * use the meta-data associated with members to identify another server from which to obtain a checkpoint file.</p>
 *
 * <p>Once a checkpoint has been recovered it should be loaded and the associated <code>CheckpointHandle</code> passed
 * into <code>bringUpToDate</code>. This will bring the replica back into sync and cause it to restart processing
 * of paxos messages in the cluster.</p>
 *
 * <p>A typical implementation would checkpoint after a certain number of operations following a process similar to:</p>
 *
 * <ol>
 *     <li>Invoke <code>newCheckpoint</code></li>
 *     <li>Obtain a checkpoint file via <code>CheckpointStorage.newCheckpoint</code></li>
 *     <li>Write current state to the checkpoint file along with the handle from <code>newCheckpoint</code></li>
 *     <li>Invoke <code></code>WriteCheckpoint.saved</code></li>
 *     <li>Invoke CheckpointHandle.saved</code></li>
 * </ol>
 *
 * <p>The most recent checkpoint file would thus be obtained from another replica and a process similar to the following
 * would then commence:</p>
 *
 * <ol>
 *     <li>Install the recovered state</li>
 *     <li>Invoke <code>CheckpointStorage.newCheckpoint</code></li>
 *     <li>Write the updated state and the <code>CheckpointHandle</code> from the remote checkpoint.=</li>
 *     <li>Invoke <code>WriteCheckpoint.saved</code></li>
 *     <li>Invoke <code>bringUpToDate</code> with the recovered CheckpointHandle</li>
 * </ol>
 *
 * @see VoteOutcome
 * @see CheckpointHandle
 */
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
    public void add(Listener aListener);
    public void bringUpToDate(CheckpointHandle aHandle) throws Exception;
    public FailureDetector getDetector();
}
