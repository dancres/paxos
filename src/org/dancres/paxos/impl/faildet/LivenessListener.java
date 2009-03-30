package org.dancres.paxos.impl.faildet;

import java.net.SocketAddress;

/**
 * Register one of these with a {@link FailureDetector} to track reports of nodes being detected or lost
 */
public interface LivenessListener {
    public void alive(SocketAddress aProcess);
    public void dead(SocketAddress aProcess);
}
