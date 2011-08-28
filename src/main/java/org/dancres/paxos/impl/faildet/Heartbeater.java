package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.impl.Transport;

/**
 * Broadcasts <code>Heartbeat</code> messages at an appropriate rate for <code>FailureDetectorImpl</code>'s in
 * other nodes. Heartbeats optionally carry metadata which an application could use e.g. to find the contact details
 * for the server that is the new leader after a paxos view change as indicated by receiving
 * <code>Event.Reason.OTHER_LEADER</code>. 
 *
 * @author dan
 */
public class Heartbeater extends Thread {
    private Transport _transport;
    private byte[] _metaData;

    private boolean _stopping = false;
    
    public Heartbeater(Transport aTransport, byte[] metaData) {
        _transport = aTransport;
        _metaData = metaData;
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
            _transport.send(new Heartbeat(_transport.getLocalAddress(), _metaData), _transport.getBroadcastAddress());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
    }
}
