package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import org.dancres.paxos.impl.core.Channel;
import org.dancres.paxos.impl.core.messages.PaxosMessage;

/**
 * Simple implementation of a channel for use in local testing scenarios that use a {@link PacketQueue}.
 * This channel is reliable and messages are delivered in the order they are submitted.  One might typically
 * reimplement {@link Channel} or extend this class to provide e.g. loss'y Channel implementations to test
 * failure scenarios under packet-loss.
 *
 * @author dan
 */
public class QueueChannelImpl implements Channel {
    private PacketQueue _queue;
    private InetSocketAddress _addr;

    /**
     * @param anAddr the source address to apply to packets entering this channel
     * @param aQueue the destination queue to place the packets in
     */
    public QueueChannelImpl(InetSocketAddress anAddr, PacketQueue aQueue) {
        _queue = aQueue;
        _addr = anAddr;
    }

    public void write(PaxosMessage aMessage) {
        _queue.add(new Packet(_addr, aMessage));
    }
}
