/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dancres.paxos.impl.io.mina;

import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.dancres.paxos.impl.core.Address;
import org.dancres.paxos.impl.faildet.FailureDetector;
import org.dancres.paxos.impl.core.messages.PaxosMessage;
import org.dancres.paxos.impl.core.ProposerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import org.dancres.paxos.impl.core.Transport;
import org.dancres.paxos.impl.core.messages.Operations;
import org.dancres.paxos.impl.util.AddressImpl;

/**
 *
 * @author dan
 */
public class ProposerAdapter extends IoHandlerAdapter {
    private ProposerImpl _proposer;
    private TransportImpl _transport;

    private Logger _logger = LoggerFactory.getLogger(ProposerAdapter.class);

    public ProposerAdapter() {
    }

    /**
     * @param aSession is the broadcast channel
     * @param aDetector is the failure detector to use for membership management etc
     * @param anAddress is the endpoint (address and port) for this node
     */
    public void init(IoSession aSession, FailureDetector aDetector, InetSocketAddress anAddress) {
        _transport = new TransportImpl(aSession);
        _proposer = new ProposerImpl(_transport, aDetector, anAddress);
    }

    public void exceptionCaught(org.apache.mina.common.IoSession aSession,
                                java.lang.Throwable aThrowable) throws java.lang.Exception {
        _logger.error("Server exp: s=" + aSession, aThrowable);
    }

    public void messageReceived(org.apache.mina.common.IoSession aSession,
                                java.lang.Object anObject) throws java.lang.Exception {

        _transport.register(aSession);

        PaxosMessage myMessage = (PaxosMessage) anObject;

        if (myMessage.getType() != Operations.HEARTBEAT)
                _logger.info("serverMsgRx: s=" + aSession + " o=" + anObject);

        _proposer.process(myMessage, new AddressImpl(aSession.getRemoteAddress()));
    }

    public void messageSent(org.apache.mina.common.IoSession aSession,
                            java.lang.Object anObject) throws java.lang.Exception {

        PaxosMessage myMessage = (PaxosMessage) anObject;

        if (myMessage.getType() != Operations.HEARTBEAT)
            _logger.info("serverMsgTx: s=" + aSession + " o=" + anObject);
    }
}
