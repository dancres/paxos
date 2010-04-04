package org.dancres.paxos;

import org.dancres.paxos.messages.PaxosMessage;

public interface Stream {
	public void close();
	public void send(PaxosMessage aMessage);
}
