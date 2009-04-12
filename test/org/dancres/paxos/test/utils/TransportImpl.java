package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.dancres.paxos.impl.core.Address;
import org.dancres.paxos.impl.core.Transport;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.util.AddressImpl;

public class TransportImpl implements Transport {
    private ConcurrentHashMap<Address, PacketQueue> _queues = new ConcurrentHashMap<Address, PacketQueue>();
    private Address _address;

    public TransportImpl(InetSocketAddress anAddress) {
        _address = new AddressImpl(anAddress);
    }

    public void add(InetSocketAddress anAddress, PacketQueue aQueue) {
        _queues.put(new AddressImpl(anAddress), aQueue);
    }

    public void send(PaxosMessage aMessage, Address anAddress) {
        if (anAddress.equals(Address.BROADCAST)) {
            Iterator<PacketQueue> myQueues = _queues.values().iterator();

            while (myQueues.hasNext())
                myQueues.next().add(new Packet(_address, aMessage));
        } else {
            _queues.get(anAddress).add(new Packet(_address, aMessage));
        }
    }
}
