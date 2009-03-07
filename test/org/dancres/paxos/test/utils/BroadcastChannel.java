package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.dancres.paxos.impl.core.Channel;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

public class BroadcastChannel implements Channel {
    private InetSocketAddress _source;
    private List<InetSocketAddress> _recipients = new ArrayList<InetSocketAddress>();
    private QueueRegistry _registry;
    private ExecutorService _executor = Executors.newSingleThreadExecutor();

    public BroadcastChannel(InetSocketAddress aSource, QueueRegistry aRegistry) {
        _registry = aRegistry;
        _source = aSource;
    }

    public void add(InetSocketAddress anAddr) {
        synchronized(_recipients) {
            _recipients.add(anAddr);
        }
    }

    public void write(PaxosMessage aMsg) {
        InetSocketAddress[] myRecipients;

        synchronized(_recipients) {
            myRecipients = new InetSocketAddress[_recipients.size()];
            myRecipients = _recipients.toArray(myRecipients);
        }

        _executor.execute(new BroadcastTask(myRecipients, _source, aMsg));
    }

    public void close() {
        _executor.shutdownNow();
    }

    private class BroadcastTask implements Runnable {
        private InetSocketAddress[] _recipients;
        private InetSocketAddress _source;
        private PaxosMessage _msg;

        BroadcastTask(InetSocketAddress[] aRecipients, InetSocketAddress aSource, PaxosMessage aMsg) {
            _recipients = aRecipients;
            _source = aSource;
            _msg = aMsg;
        }

        public void run() {
            for (int i = 0; i < _recipients.length; i++) {
                PacketQueue myQueue = _registry.getQueue(_recipients[i]);
                myQueue.add(new Packet(_source, _msg));
            }
        }
    }
}
