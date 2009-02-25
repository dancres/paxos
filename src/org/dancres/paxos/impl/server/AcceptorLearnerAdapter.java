package org.dancres.paxos.impl.server;

import org.apache.mina.common.*;
import org.apache.mina.transport.socket.DatagramConnector;
import org.dancres.paxos.impl.messages.*;
import org.dancres.paxos.impl.core.AcceptorLearnerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class AcceptorLearnerAdapter implements IoHandler {
    private Logger _logger = LoggerFactory.getLogger(AcceptorLearnerAdapter.class);

    private DatagramConnector _propUnicast;
    private AcceptorLearnerImpl _acceptorLearner;

    public AcceptorLearnerAdapter(DatagramConnector aPropUnicast) {
        _propUnicast = aPropUnicast;
        _acceptorLearner = new AcceptorLearnerImpl();
    }

    public void sessionCreated(IoSession ioSession) throws Exception {
        _logger.info("Session created:" + ioSession);
    }

    public void sessionOpened(IoSession ioSession) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void sessionClosed(IoSession ioSession) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void sessionIdle(IoSession ioSession, IdleStatus idleStatus) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
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
