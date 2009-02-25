package org.dancres.paxos.test;

import org.apache.mina.transport.socket.DatagramConnector;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;
import org.apache.mina.common.*;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.dancres.paxos.impl.codec.PaxosCodecFactory;
import org.dancres.paxos.impl.messages.Post;

import java.net.InetSocketAddress;

public class PaxosClient implements IoHandler {
    private PaxosClient() {

    }

    private void test(String[] anArgs) throws Exception {
        DatagramConnector myConnector = new NioDatagramConnector();
        myConnector.setHandler(this);
        myConnector.getFilterChain().addLast( "logger", new LoggingFilter() );
        myConnector.getFilterChain().addLast("protocol",
                new ProtocolCodecFilter(new PaxosCodecFactory()));
        ConnectFuture connFuture =
                myConnector.connect(new InetSocketAddress(anArgs[0], Integer.parseInt(anArgs[1])));
        connFuture.awaitUninterruptibly();
        IoSession mySession = connFuture.getSession();

        for (int i = 0; i < 1; i++) {
            IoBuffer buffer = IoBuffer.allocate(4);
            buffer.putInt(i);
            buffer.flip();

            mySession.write(new Post(buffer.array()));

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
    }

    public static void main(String[] anArgs) throws Exception {
        new PaxosClient().test(anArgs);
    }

    public void sessionCreated(IoSession ioSession) throws Exception {
        //To change body of implemented methods use File | Settings | File Templates.
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
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void messageReceived(IoSession ioSession, Object object) throws Exception {
        System.err.println("Rcvd: " + ioSession + " " + object);
    }

    public void messageSent(IoSession ioSession, Object object) throws Exception {
        System.err.println("Sent: " + ioSession + " " + object);
    }
}
