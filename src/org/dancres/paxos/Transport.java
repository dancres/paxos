package org.dancres.paxos;

import org.dancres.paxos.messages.PaxosMessage;

/**
 * Standard communications abstraction for all communication between leaders, acceptor/learners and clients.
 *
 * @author dan
 */
public interface Transport {
	public NodeId getLocalNodeId();
	
    /**
     * @param aMessage is the message to send
     * @param aNodeId is the abstract address of the target for the message which might be <code>Address.BROADCAST</code>.
     */
    public void send(PaxosMessage aMessage, NodeId aNodeId);
}
