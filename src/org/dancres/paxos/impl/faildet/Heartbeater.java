package org.dancres.paxos.impl.faildet;

import org.dancres.paxos.impl.core.Channel;
import org.dancres.paxos.impl.messages.Heartbeat;

public class Heartbeater implements Runnable {
    private Channel _channel;

    public Heartbeater(Channel aChannel) {
        _channel = aChannel;
    }

    public void run() {
        while (true) {
            _channel.write(new Heartbeat());
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
        }
    }
}
