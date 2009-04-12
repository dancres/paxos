package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.impl.core.Address;

/**
 * Register one of these with a {@link FailureDetector} to track reports of nodes being detected or lost
 */
public interface LivenessListener {
    public void alive(Address aProcess);
    public void dead(Address aProcess);
}
