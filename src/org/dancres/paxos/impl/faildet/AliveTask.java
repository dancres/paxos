package org.dancres.paxos.impl.faildet;

import java.util.concurrent.CopyOnWriteArraySet;
import java.util.Iterator;
import org.dancres.paxos.impl.core.Address;

class AliveTask implements Runnable {
    private Address _address;
    private CopyOnWriteArraySet _listeners;

    public AliveTask(Address aRemoteAddress, CopyOnWriteArraySet aListeners) {
        _address = aRemoteAddress;
        _listeners = aListeners;
    }

    public void run() {
        Iterator myListeners = _listeners.iterator();
        while (myListeners.hasNext()) {
            LivenessListener myListener = (LivenessListener) myListeners.next();
            myListener.alive(_address);
        }
    }
}
