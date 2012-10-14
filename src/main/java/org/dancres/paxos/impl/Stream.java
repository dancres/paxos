package org.dancres.paxos.impl;

import org.dancres.paxos.messages.PaxosMessage;

public interface Stream {
	public void close();
	public void send(PaxosMessage aMessage);
	public void sendRaw(Transport.Packet aPacket);
}
