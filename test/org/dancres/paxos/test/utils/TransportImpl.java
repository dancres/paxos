package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.dancres.paxos.Transport;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.NodeId;

public class TransportImpl implements Transport {
    private ConcurrentHashMap<NodeId, PacketQueue> _queues = new ConcurrentHashMap<NodeId, PacketQueue>();
    private NodeId _address;
    private int _port;

    public TransportImpl(InetSocketAddress anAddress) {
        _address = NodeId.from(anAddress);
        _port = anAddress.getPort();
    }

    public void add(InetSocketAddress anAddress, PacketQueue aQueue) {
        _queues.put((NodeId) NodeId.from(anAddress), aQueue);
    }

    public void send(PaxosMessage aMessage, NodeId anAddress) {
        if (anAddress.equals(NodeId.BROADCAST)) {
            Iterator<PacketQueue> myQueues = _queues.values().iterator();

            while (myQueues.hasNext())
                myQueues.next().add(new Packet(_address, aMessage));
        } else {
            _queues.get(anAddress).add(new Packet(_address, aMessage));
        }
    }
}
