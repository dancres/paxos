package org.dancres.paxos.impl.io.mina;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.mina.common.IoSession;
import org.dancres.paxos.impl.core.Address;
import org.dancres.paxos.impl.core.Transport;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.util.AddressImpl;

public class TransportImpl implements Transport {

    private ConcurrentHashMap<Address, IoSession> _sessions = new ConcurrentHashMap<Address, IoSession>();

    public TransportImpl(IoSession aBroadcastSession) {
        super();
        _sessions.put(Address.BROADCAST, aBroadcastSession);
    }

    public void send(PaxosMessage aMessage, Address anAddress) {
        IoSession mySession = (IoSession) _sessions.get(anAddress);
        mySession.write(aMessage);
    }

    public void register(IoSession aSession) {
        _sessions.putIfAbsent(new AddressImpl(aSession.getRemoteAddress()), aSession);
    }
}
