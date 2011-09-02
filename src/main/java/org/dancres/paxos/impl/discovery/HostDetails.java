package org.dancres.paxos.impl.discovery;

import java.net.InetSocketAddress;

public class HostDetails {
	private String _host;
	private int _port;
	
	public HostDetails(String aHostName, int aPort) {
		_host = aHostName;
		_port = aPort;
	}
	
	public String toString() {
		return _host + ":" + _port;
	}
	
	public InetSocketAddress getAddr() {
		return new InetSocketAddress(_host, _port);
	}
}
