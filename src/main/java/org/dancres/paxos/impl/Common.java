package org.dancres.paxos.impl;

import org.dancres.paxos.Listener;
import org.dancres.paxos.StateEvent;
import org.dancres.paxos.bus.Messages;
import org.dancres.paxos.bus.MessagesImpl;

import java.util.Timer;

public class Common {
    private Transport _transport;
    private final Timer _watchdog = new Timer("Paxos timers");
    private final NodeState _nodeState = new NodeState();
    private final Messages<Constants.EVENTS> _bus = new MessagesImpl<>();

    Messages<Constants.EVENTS> getBus() {
        return _bus;
    }

    Common setTransport(Transport aTransport) {
        _transport = aTransport;
        return this;
    }
    
    Timer getWatchdog() {
        return _watchdog;
    }

    public Transport getTransport() {
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

    void addStateEventListener(Listener aListener) {
        _bus.anonSubscrbe((aMessage) -> {
            switch(aMessage.getType()) {
                case AL_TRANSITION : {
                    aListener.transition((StateEvent) aMessage.getMessage());
                    break;
                }
            }
        });
    }
}
