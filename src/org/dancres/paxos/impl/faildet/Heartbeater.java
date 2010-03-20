package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.Transport;
import org.dancres.paxos.NodeId;

/**
 * Broadcasts <code>Heartbeat</code> messages at an appropriate rate for <code>FailureDetectorImpl</code>'s in other nodes.
 *
 * @author dan
 */
public class Heartbeater implements Runnable {
    private Transport _transport;
    
    public Heartbeater(Transport aTransport) {
        _transport = aTransport;
    }

    public void run() {
        while (true) {
            _transport.send(new Heartbeat(_transport.getLocalNodeId().asLong()), NodeId.BROADCAST);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
    }
}
