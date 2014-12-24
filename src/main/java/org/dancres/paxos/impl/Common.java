package org.dancres.paxos.impl;

import org.dancres.paxos.Listener;
import org.dancres.paxos.StateEvent;

import java.util.List;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArrayList;

class Common {
    private Transport _transport;
    private final Timer _watchdog = new Timer("Paxos timers");
    private final NodeState _nodeState = new NodeState();
    private final List<Listener> _listeners = new CopyOnWriteArrayList<>();

    Common(Transport aTransport) {
        _transport = aTransport;
    }

    Common() {
        this(null);
    }
    
    void setTransport(Transport aTransport) {
    	_transport = aTransport;
    }
    
    Timer getWatchdog() {
        return _watchdog;
    }

    Transport getTransport() {
        return _transport;
    }

    void stop() {
        _watchdog.cancel();
    }
    
    boolean amMember() {
        return _transport.getFD().isMember(_transport.getLocalAddress());
    }

    NodeState getNodeState() {
        return _nodeState;
    }

    void add(Listener aListener) {
        _listeners.add(aListener);
    }

    void signal(StateEvent aStatus) {
        for (Listener myTarget : _listeners)
            myTarget.transition(aStatus);
    }
}
