package org.dancres.paxos.test.utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.dancres.paxos.impl.NetworkUtils;

/**
 * Manages address/recipient mappings for local unit tests - note that it requires an active network interface to source
 * an ip address from (via NetworkUtils).
 * @author dan
 */
public class AddressRegistry {
    private int _nextPort = 1024;
    private Map _addrRecipientMap = new HashMap();
    private InetAddress _addr;

    public AddressRegistry() throws Exception {
        _addr = NetworkUtils.getWorkableInterface();
    }

    public SocketAddress allocate(Object aRecipient) {
        synchronized(this) {
            int myPort = _nextPort++;

            SocketAddress myNewAddr = new InetSocketAddress(_addr, myPort);
            _addrRecipientMap.put(myNewAddr, aRecipient);

            return myNewAddr;
        }
    }
}
