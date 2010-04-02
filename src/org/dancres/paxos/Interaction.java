package org.dancres.paxos;

import org.dancres.paxos.messages.PaxosMessage;

public interface Interaction {
	public void messageReceived(PaxosMessage aMessage);
}
