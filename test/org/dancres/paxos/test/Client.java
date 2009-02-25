package org.dancres.paxos.test;

import org.apache.mina.common.*;
import org.apache.mina.transport.socket.DatagramConnector;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Client implements IoHandler {
    void main(String anArgs[]) throws IOException {
        DatagramConnector myConnector = new NioDatagramConnector();
        myConnector.getSessionConfig().setBroadcast(true);
        myConnector.setHandler(this);
        ConnectFuture connFuture =
                myConnector.connect(new InetSocketAddress("192.168.0.255", 41952));
        System.err.println(myConnector);
        connFuture.awaitUninterruptibly();
        IoSession mySession = connFuture.getSession();

        for (int i = 0; i < 30; i++) {
            long free = Runtime.getRuntime().freeMemory();
            IoBuffer buffer = IoBuffer.allocate(8);
            buffer.putLong(free);
            buffer.flip();

            mySession.write(buffer);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }
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
