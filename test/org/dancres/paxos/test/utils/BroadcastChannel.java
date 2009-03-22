package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.dancres.paxos.impl.core.Channel;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

/**
 * A local emulation of a broadcast network.  Maintains a list of addresses to broadcast to
 * and finds the associated nodes via a {@link ChannelRegistry}.
 *
 * @author dan
 */
public class BroadcastChannel implements Channel {
    private List<InetSocketAddress> _recipients = new ArrayList<InetSocketAddress>();
    private ChannelRegistry _registry;
    private ExecutorService _executor = Executors.newSingleThreadExecutor();

    /**
     * @param aRegistry is the registry in which to lookup channels for addresses in the broadcast group
     */
    public BroadcastChannel(ChannelRegistry aRegistry) {
        _registry = aRegistry;
    }

    /**
     * @param anAddr is the address to add to the broadcast set.
     */
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
