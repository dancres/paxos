package org.dancres.paxos.test.utils;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.dancres.paxos.impl.core.Channel;

/**
 * Tracks the mappings for one sender to all its potential recipients.  Each Channel knows who its sender is and
 * has access to the appropriate recipient endpoint.
 *
 * @author dan
 */
public class ChannelRegistry {
    private Map<SocketAddress, Channel> _addrRecipientMap = new HashMap<SocketAddress, Channel>();

    public ChannelRegistry() {
    }

    /**
     * Register a source/endpoint pair for a recipient.
     *
     * @param anAddress is the address of the recipient
     * @param aRecipient is the channel on which the recipient may be found
     */
    public void register(SocketAddress anAddress, Channel aRecipient) {
        synchronized(this) {
            _addrRecipientMap.put(anAddress, aRecipient);
        }
    }

    /**
     * Recover a channel for the specified address
     *
     * @param anAddr is the address of the desired recipient
     * @return the associated channel
     */
    public Channel getChannel(SocketAddress anAddr) {
        synchronized(this) {
            return _addrRecipientMap.get(anAddr);
        }
    }
}
