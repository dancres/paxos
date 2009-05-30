package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.dancres.paxos.Address;
import org.dancres.paxos.Transport;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.impl.mina.io.ProposerHeader;
import org.dancres.paxos.impl.util.NodeId;

public class TransportImpl implements Transport {
    private ConcurrentHashMap<Address, PacketQueue> _queues = new ConcurrentHashMap<Address, PacketQueue>();
    private Address _address;
    private int _port;

    public TransportImpl(InetSocketAddress anAddress) {
        _address = NodeId.from(anAddress);
        _port = anAddress.getPort();
    }

    public void add(InetSocketAddress anAddress, PacketQueue aQueue) {
        _queues.put((Address) NodeId.from(anAddress), aQueue);
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
