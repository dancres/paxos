package org.dancres.paxos.impl.mina.io;

import org.apache.mina.common.*;
import org.apache.mina.transport.socket.DatagramConnector;
import org.dancres.paxos.messages.*;
import org.dancres.paxos.AcceptorLearner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class AcceptorLearnerAdapter extends IoHandlerAdapter {
    private Logger _logger = LoggerFactory.getLogger(AcceptorLearnerAdapter.class);

    private DatagramConnector _propUnicast;
    private AcceptorLearner _acceptorLearner;

    public AcceptorLearnerAdapter(DatagramConnector aPropUnicast, AcceptorLearner anAl) {
        _propUnicast = aPropUnicast;
        _acceptorLearner = anAl;
    }

    public void sessionCreated(IoSession ioSession) throws Exception {
        _logger.info("Session created:" + ioSession);
    }

    public void exceptionCaught(IoSession ioSession, Throwable throwable) throws Exception {
        _logger.error("Client got exception:", throwable);
    }

    public void messageReceived(IoSession aSession, Object anObject) throws Exception {
        _logger.info("clientMsgRx: s=" + aSession + " o=" + anObject);

        ProposerPacket myMessage = (ProposerPacket) anObject;
        PaxosMessage myOperation = (PaxosMessage) myMessage.getOperation();
        
        PaxosMessage myResponse = _acceptorLearner.process(myOperation);

        if (myResponse != null) {
            InetSocketAddress mySource = (InetSocketAddress) aSession.getRemoteAddress();

            _logger.info("Attempting to return message on: " + mySource.getAddress() + " -> " + myMessage.getPort());
            ConnectFuture connFuture =
                    _propUnicast.connect(new InetSocketAddress(mySource.getAddress(), myMessage.getPort()));
            connFuture.awaitUninterruptibly();
            IoSession mySession = connFuture.getSession();

            mySession.write(myResponse);
            mySession.close();
        }
    }

    public void messageSent(IoSession aSession, Object anObject) throws Exception {
        _logger.info("clientMsgTx: s=" + aSession + " o=" + anObject);
    }
}
