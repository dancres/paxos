package org.dancres.paxos;

import org.dancres.paxos.impl.util.NodeId;
import org.dancres.paxos.messages.PaxosMessage;

/**
 * Standard communications abstraction for all communication between leaders, acceptor/learners and clients.
 *
 * @author dan
 */
public interface Transport {
    /**
     * @param aMessage is the message to send
     * @param anAddress is the abstract address of the target for the message which might be <code>Address.BROADCAST</code>.
     */
    public void send(PaxosMessage aMessage, NodeId aNodeId);
}
