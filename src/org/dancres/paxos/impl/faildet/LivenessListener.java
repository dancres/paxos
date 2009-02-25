package org.dancres.paxos.impl.faildet;

import java.net.SocketAddress;

public interface LivenessListener {
    public void alive(SocketAddress aProcess);
    public void dead(SocketAddress aProcess);
}
