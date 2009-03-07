package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import org.dancres.paxos.impl.core.Channel;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

public class QueueChannelImpl implements Channel {
    private PacketQueue _queue;
    private InetSocketAddress _addr;

    public QueueChannelImpl(InetSocketAddress anAddr, PacketQueue aQueue) {
        _queue = aQueue;
        _addr = anAddr;
    }

    public void write(PaxosMessage aMessage) {
        _queue.add(new Packet(_addr, aMessage));
    }
}
