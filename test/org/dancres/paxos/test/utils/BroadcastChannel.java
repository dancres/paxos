package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.dancres.paxos.impl.core.Channel;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

public class BroadcastChannel implements Channel {
    private List<InetSocketAddress> _recipients = new ArrayList<InetSocketAddress>();
    private ChannelRegistry _registry;
    private ExecutorService _executor = Executors.newSingleThreadExecutor();

    public BroadcastChannel(ChannelRegistry aRegistry) {
        _registry = aRegistry;
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

        _executor.execute(new BroadcastTask(myRecipients, aMsg));
    }

    public void close() {
        _executor.shutdownNow();
    }

    private class BroadcastTask implements Runnable {
        private InetSocketAddress[] _recipients;
        private PaxosMessage _msg;

        BroadcastTask(InetSocketAddress[] aRecipients, PaxosMessage aMsg) {
            _recipients = aRecipients;
            _msg = aMsg;
        }

        public void run() {
            for (int i = 0; i < _recipients.length; i++) {
                Channel myChannel = _registry.getChannel(_recipients[i]);
                myChannel.write(_msg);
            }
        }
    }
}
