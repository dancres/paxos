package org.dancres.paxos.impl.discovery;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Represents the basic membership operations.
 *
 * @author dan
 */
public interface Registrar {
	public static final String TYPE = "_paxos._udp";

    /**
     * Advertise an address to other nodes
     * @param localAddress is the address to advertise
     *
     * @throws java.io.IOException
     */
	public void register(InetSocketAddress localAddress) throws IOException;

    /**
     * Locate other Paxos nodes
     *
     * @param aTimePeriod is the maximum time to search for other nodes.
     * @return details of any nodes found
     *
     * @throws java.io.IOException
     */
	public HostDetails[] find(long aTimePeriod) throws IOException;
}
