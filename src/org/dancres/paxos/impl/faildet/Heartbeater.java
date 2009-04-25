package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.impl.core.Address;
import org.dancres.paxos.impl.core.Transport;
import org.dancres.paxos.impl.faildet.Heartbeat;

public class Heartbeater implements Runnable {
    private Transport _transport;

    public Heartbeater(Transport aTransport) {
        _transport = aTransport;
    }

    public void run() {
        while (true) {
            _transport.send(new Heartbeat(), Address.BROADCAST);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
    }
}
