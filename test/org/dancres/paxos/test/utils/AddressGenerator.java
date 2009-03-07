package org.dancres.paxos.test.utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.dancres.paxos.impl.NetworkUtils;

/**
 * Allocates local addresses - note that it requires an active network interface to source
 * an ip address from (via NetworkUtils).
 * @author dan
 */
public class AddressGenerator {
    private int _nextPort = 1024;
    private InetAddress _addr;

    public AddressGenerator() throws Exception {
        _addr = NetworkUtils.getWorkableInterface();
    }

    public InetSocketAddress allocate() {
        synchronized(this) {
            int myPort = _nextPort++;

            return new InetSocketAddress(_addr, myPort);
        }
    }
}
