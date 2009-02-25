package org.dancres.paxos.impl.messages;

public class ProposerHeader implements ProposerPacket {
	private int _port;
	private PaxosMessage _operation;
	
	public ProposerHeader(PaxosMessage aMessage, int aPort) {
		_port = aPort;
		_operation = aMessage;
	}
	
	public int getPort() {
		return _port;
	}

	public PaxosMessage getOperation() {
		return _operation;
	}

	public long getSeqNum() {
		return _operation.getSeqNum();
	}

	public int getType() {
		return Operations.PROPOSER_REQ;
	}
	
	public String toString() {
		return "PH: " + _port + " -> " + _operation;
	}
}
