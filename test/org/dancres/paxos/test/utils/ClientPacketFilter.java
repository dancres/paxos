package org.dancres.paxos.test.utils;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

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

    public void add(PaxosMessage aMessage) {
        switch (aMessage.getType()) {
            case Operations.COMPLETE :
            case Operations.FAIL : {
                _queue.add(aMessage);
                break;
            }
        }
    }

    public PaxosMessage getNext(long aPause) throws InterruptedException {
        return _queue.getNext(aPause);
    }
}
