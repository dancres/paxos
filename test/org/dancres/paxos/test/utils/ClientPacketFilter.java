package org.dancres.paxos.test.utils;

import org.dancres.paxos.messages.Operations;

/**
 * Real clients would not see anything other than ACK or FAIL but the local test network simulations will generate additional
 * traffic (broadcasts) to the client which need to be ignored.  This filter performs that tasks and simplifies the writing
 * of unit tests.
 */
public class ClientPacketFilter implements PacketQueue {
    private PacketQueue _queue;

    public ClientPacketFilter(PacketQueue aQueue) {
        _queue = aQueue;
    }

    public void add(Packet aPacket) {
        switch (aPacket.getMsg().getType()) {
            case Operations.ACK :
            case Operations.FAIL : {
                _queue.add(aPacket);
                break;
            }
        }
    }

    public Packet getNext(long aPause) throws InterruptedException {
        return _queue.getNext(aPause);
    }
}
