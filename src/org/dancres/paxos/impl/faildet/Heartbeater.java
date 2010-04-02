package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.Transport;
import org.dancres.paxos.NodeId;

/**
 * Broadcasts <code>Heartbeat</code> messages at an appropriate rate for <code>FailureDetectorImpl</code>'s in other nodes.
 *
 * @author dan
 */
public class Heartbeater extends Thread {
    private Transport _transport;
    private boolean _stopping = false;
    
    public Heartbeater(Transport aTransport) {
        _transport = aTransport;
    }

    public void halt() {
    	synchronized(this) {
    		_stopping = true;
    	}
    }
    
    private boolean isStopping() {
    	synchronized(this) {
    		return _stopping;
    	}
    }
    
    public void run() {
        while (! isStopping()) {
            _transport.send(new Heartbeat(_transport.getLocalNodeId().asLong()), NodeId.BROADCAST);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
    }
}
