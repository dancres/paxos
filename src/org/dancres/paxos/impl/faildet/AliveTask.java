package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.LivenessListener;

import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.Iterator;

class AliveTask implements Runnable {
    private InetSocketAddress _nodeId;
    private CopyOnWriteArraySet _listeners;

    public AliveTask(InetSocketAddress aRemoteAddress, CopyOnWriteArraySet aListeners) {
        _nodeId = aRemoteAddress;
        _listeners = aListeners;
    }

    public void run() {
        Iterator myListeners = _listeners.iterator();
        while (myListeners.hasNext()) {
            LivenessListener myListener = (LivenessListener) myListeners.next();
            myListener.alive(_nodeId);
        }
    }
}
