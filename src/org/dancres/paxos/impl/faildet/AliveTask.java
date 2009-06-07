package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.LivenessListener;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.Iterator;
import org.dancres.paxos.NodeId;

class AliveTask implements Runnable {
    private NodeId _nodeId;
    private CopyOnWriteArraySet _listeners;

    public AliveTask(NodeId aRemoteAddress, CopyOnWriteArraySet aListeners) {
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
