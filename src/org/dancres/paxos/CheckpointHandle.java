package org.dancres.paxos;

import java.io.Serializable;

/**
 * A checkpoint constitutes a snapshot of state at a particular moment in time, t. Once a checkpoint is sufficiently
 * persisted (e.g. replicated etc) all log entries representing state transitions prior to t can be discarded. This
 * ensures a Paxos implementation doesn't consume infinite disk-space over time.
 *
 * A checkpoint handle refers to a snapshot at time t and is created by invoking an appropriate method within the Paxos
 * framework. Once the handle has been saved together with other state, the application should call <code>saved</code>.
 * Note that if a checkpoint handle is lost log records will be retained until such time as another checkpoint handle
 * is created and saved successfully.
 *
 * When the Paxos framework is initialised, the last saved checkpoint handle (or <code>NO_CHECKPOINT</code>) should
 * be passed in. It is assumed that the log files required to recover from the checkpoint forwards are present.
 * The framework can manage without these log files and recover online so long as the checkpoint state is sufficiently
 * recent.
 */
public abstract class CheckpointHandle implements Serializable {
    public static final CheckpointHandle NO_CHECKPOINT =
            new CheckpointHandle() {
                public void saved() {}};

    public abstract void saved() throws Exception;
}
