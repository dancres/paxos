/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dancres.paxos.impl.mina.io;

import org.apache.mina.common.IoHandlerAdapter;
import org.apache.mina.common.IoSession;
import org.dancres.paxos.impl.faildet.FailureDetectorImpl;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.Leader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import org.dancres.paxos.AcceptorLearner;
import org.dancres.paxos.Operation;
import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.NodeId;

/**
 *
 * @author dan
 */
public class ProposerAdapter extends IoHandlerAdapter {
    private Leader _leader;
    private TransportImpl _transport;

    private Logger _logger = LoggerFactory.getLogger(ProposerAdapter.class);

    public ProposerAdapter() {
    }

    /**
     * @param aSession is the broadcast channel
     * @param aDetector is the failure detector to use for membership management etc
     * @param anAddress is the endpoint (address and port) for this node
     */
    public void init(IoSession aSession, FailureDetectorImpl aDetector, InetSocketAddress anAddress, AcceptorLearner anAl) {
        _transport = new TransportImpl(anAddress, aSession);
        _leader = new Leader(aDetector, NodeId.from(anAddress), _transport, anAl);
    }

    public void exceptionCaught(org.apache.mina.common.IoSession aSession,
                                java.lang.Throwable aThrowable) throws java.lang.Exception {
        _logger.error("Server exp: s=" + aSession, aThrowable);
    }

    public void messageReceived(org.apache.mina.common.IoSession aSession,
                                java.lang.Object anObject) throws java.lang.Exception {

        _transport.register(aSession);

        PaxosMessage myMessage = (PaxosMessage) anObject;

        if (myMessage.getType() != Heartbeat.TYPE)
                _logger.info("serverMsgRx: s=" + aSession + " o=" + anObject);

        if (myMessage.getType() == Post.TYPE)
            _leader.submit(new Operation(((Post) myMessage).getValue(), new byte[0]));
        else
            _leader.messageReceived(myMessage, NodeId.from(aSession.getRemoteAddress()));
    }

    public void messageSent(org.apache.mina.common.IoSession aSession,
                            java.lang.Object anObject) throws java.lang.Exception {

        PaxosMessage myMessage = (PaxosMessage) anObject;

        if (myMessage.getType() != Heartbeat.TYPE)
            _logger.info("serverMsgTx: s=" + aSession + " o=" + anObject);
    }
}
