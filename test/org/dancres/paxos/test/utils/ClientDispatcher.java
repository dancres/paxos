package org.dancres.paxos.test.utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.Transport;
import org.dancres.paxos.impl.netty.TransportImpl;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

public class ClientDispatcher implements TransportImpl.Dispatcher {
	private Transport _transport;
	private List<PaxosMessage> _queue = new ArrayList<PaxosMessage>();
	
	public void messageReceived(PaxosMessage aMessage) {
		synchronized(this) {
	        switch (aMessage.getType()) {
	        	case Operations.COMPLETE :
	        	case Operations.FAIL : {
	        		_queue.add(aMessage);
	        		notifyAll();
	        		break;
	        	}
            }
        }
	}

	public void send(PaxosMessage aMessage, InetSocketAddress aTarget) {
		_transport.send(aMessage, aTarget);
	}
	
	public PaxosMessage getNext(long aTimeout) {
		synchronized(this) {
			while (_queue.isEmpty()) {
				try {
					wait(aTimeout);
				} catch (InterruptedException anIE) {					
				}
			}
			
			return _queue.remove(0);
		}
	}
	
	public void setTransport(Transport aTransport) {
		_transport = aTransport;
	}
	
	public void shutdown() {
		_transport.shutdown();
	}
}
