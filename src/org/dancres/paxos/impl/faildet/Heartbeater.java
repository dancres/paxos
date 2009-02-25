package org.dancres.paxos.impl.faildet;

import org.apache.mina.common.IoSession;
import org.dancres.paxos.impl.messages.Heartbeat;

public class Heartbeater implements Runnable {
    private IoSession _session;

    public Heartbeater(IoSession aSession) {
        _session = aSession;
    }

    public void run() {
        while (true) {
            _session.write(new Heartbeat());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
    }
}
