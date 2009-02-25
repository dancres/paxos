package org.dancres.paxos.impl.faildet;

import java.util.concurrent.CopyOnWriteArraySet;
import java.util.Iterator;
import java.net.SocketAddress;

class AliveTask implements Runnable {
    private SocketAddress _address;
    private CopyOnWriteArraySet _listeners;

    public AliveTask(SocketAddress aRemoteAddress, CopyOnWriteArraySet aListeners) {
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
