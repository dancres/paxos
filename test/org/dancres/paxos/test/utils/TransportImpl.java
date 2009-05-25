package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.dancres.paxos.impl.core.Address;
import org.dancres.paxos.impl.core.Transport;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.io.mina.ProposerHeader;
import org.dancres.paxos.impl.util.AddressImpl;

public class TransportImpl implements Transport {
    private ConcurrentHashMap<Address, PacketQueue> _queues = new ConcurrentHashMap<Address, PacketQueue>();
    private Address _address;
    private int _port;

    public TransportImpl(InetSocketAddress anAddress) {
        _address = new AddressImpl(anAddress);
        _port = anAddress.getPort();
    }

    public void add(InetSocketAddress anAddress, PacketQueue aQueue) {
        _queues.put(new AddressImpl(anAddress), aQueue);
    }

    public void send(PaxosMessage aMessage, Address anAddress) {
        PaxosMessage myMessage;

        switch (aMessage.getType()) {
            case Operations.COLLECT :
            case Operations.BEGIN :
            case Operations.SUCCESS : {
                myMessage = new ProposerHeader(aMessage, _port);
                break;
            }

            default : {
                 myMessage = aMessage;
                 break;
            }
        }

        if (anAddress.equals(Address.BROADCAST)) {
            Iterator<PacketQueue> myQueues = _queues.values().iterator();

            while (myQueues.hasNext())
                myQueues.next().add(new Packet(_address, myMessage));
        } else {
            _queues.get(anAddress).add(new Packet(_address, myMessage));
        }
    }
}
