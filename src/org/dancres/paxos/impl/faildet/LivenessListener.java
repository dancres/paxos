package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.impl.util.NodeId;

/**
 * Register one of these with a {@link FailureDetector} to track reports of nodes being detected or lost
 */
public interface LivenessListener {
    public void alive(NodeId aProcess);
    public void dead(NodeId aProcess);
}
