package org.dancres.paxos.impl.mina.io;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.mina.common.IoSession;
import org.dancres.paxos.Transport;
import org.dancres.paxos.impl.util.NodeId;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

public class TransportImpl implements Transport {

    private ConcurrentHashMap<NodeId, IoSession> _sessions = new ConcurrentHashMap<NodeId, IoSession>();
    private InetSocketAddress _addr;

    public TransportImpl(InetSocketAddress aNodeAddr, IoSession aBroadcastSession) {
        super();
        _sessions.put(NodeId.BROADCAST, aBroadcastSession);
        _addr = aNodeAddr;
    }

    public void send(PaxosMessage aMessage, NodeId anAddress) {
        PaxosMessage myMessage;

        switch (aMessage.getType()) {
            case Operations.COLLECT :
            case Operations.BEGIN :
            case Operations.SUCCESS : {
                myMessage = new ProposerHeader(aMessage, _addr.getPort());
                break;
            }

            default : {
                 myMessage = aMessage;
                 break;
            }
        }

        IoSession mySession = (IoSession) _sessions.get(anAddress);
        mySession.write(myMessage);
    }

    public void register(IoSession aSession) {
        _sessions.putIfAbsent(NodeId.from(aSession.getRemoteAddress()), aSession);
    }
}
