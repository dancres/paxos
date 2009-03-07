package org.dancres.paxos.test.utils;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

public class QueueRegistry {
    private Map<SocketAddress, PacketQueue> _addrRecipientMap = new HashMap<SocketAddress, PacketQueue>();

    public QueueRegistry() {
    }

    public void register(SocketAddress anAddress, PacketQueue aRecipient) {
        synchronized(this) {
            _addrRecipientMap.put(anAddress, aRecipient);
        }
    }

    public PacketQueue[] getQueues() {
        PacketQueue[] myRecipients;

        synchronized(this) {
            myRecipients = new PacketQueue[_addrRecipientMap.size()];
            myRecipients = _addrRecipientMap.values().toArray(myRecipients);
        }

        return myRecipients;
    }

    public PacketQueue getQueue(SocketAddress anAddr) {
        synchronized(this) {
            return _addrRecipientMap.get(anAddr);
        }
    }
}
