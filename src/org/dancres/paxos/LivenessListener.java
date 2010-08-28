package org.dancres.paxos;

import java.net.InetSocketAddress;

/**
 * Register one of these with a {@link FailureDetector} to track reports of nodes being detected or lost
 */
public interface LivenessListener {
    public void alive(InetSocketAddress aProcess);
    public void dead(InetSocketAddress aProcess);
}
