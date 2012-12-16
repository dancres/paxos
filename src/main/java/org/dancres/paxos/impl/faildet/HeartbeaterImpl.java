package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Heartbeater;

/**
 * Broadcasts <code>Heartbeat</code> messages at an appropriate rate for <code>FailureDetectorImpl</code>'s in
 * other nodes. Heartbeats optionally carry metadata which an application could use e.g. to find the contact details
 * for the server that is the new leader after a paxos view change as indicated by receiving
 * <code>Event.Reason.OTHER_LEADER</code>. 
 *
 * @author dan
 */
class HeartbeaterImpl extends Thread implements Heartbeater {
    private Transport _transport;
    private byte[] _metaData;
    private long _pulseRate;

    private boolean _stopping = false;
    
    HeartbeaterImpl(Transport aTransport, byte[] metaData, long aPulseRate) {
        _transport = aTransport;
        _metaData = metaData;
        _pulseRate = aPulseRate;
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
            _transport.send(new Heartbeat(_metaData), _transport.getBroadcastAddress());

            try {
                Thread.sleep(_pulseRate);
            } catch (InterruptedException e) {}
        }
    }
}
