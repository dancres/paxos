package org.dancres.paxos.impl.discovery;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface Registrar {
	public static final String TYPE = "_paxos._udp";

	public void register(InetSocketAddress localAddress) throws IOException;
	public HostDetails[] find(long aTimePeriod) throws IOException;
}
