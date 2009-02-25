package org.dancres.paxos.test;

import org.apache.mina.common.*;
import org.apache.mina.transport.socket.DatagramAcceptor;
import org.apache.mina.transport.socket.DatagramSessionConfig;
import org.apache.mina.transport.socket.nio.NioDatagramAcceptor;
import org.apache.mina.filter.logging.LoggingFilter;

import java.net.InetSocketAddress;

public class Server implements IoHandler {
    public void main(String anArgs[]) throws Exception {
        DatagramAcceptor myAcceptor = new NioDatagramAcceptor();
        myAcceptor.setHandler(this);
        myAcceptor.getFilterChain().addLast( "logger", new LoggingFilter() );

        DatagramSessionConfig myDcfg = myAcceptor.getSessionConfig();
        myDcfg.setReuseAddress(true);
        myAcceptor.bind(new InetSocketAddress(41952));
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
        long free = Runtime.getRuntime().freeMemory();
        IoBuffer buffer = IoBuffer.allocate(8);
        buffer.putLong(free);
        buffer.flip();

        ioSession.write(buffer);        
    }

    public void messageSent(IoSession ioSession, Object object) throws Exception {
        System.err.println("Sent: " + ioSession + " " + object);
    }
}
