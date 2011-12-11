package org.dancres.paxos.impl.net;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

public class ClientDispatcher implements Transport.Dispatcher {
	private Transport _transport;
	private List<VoteOutcome> _queue = new ArrayList<VoteOutcome>();
	
	public boolean messageReceived(PaxosMessage aMessage) {
		synchronized(this) {
	        switch (aMessage.getType()) {
	        	case Operations.EVENT : {
	        		_queue.add((VoteOutcome) aMessage);
	        		notifyAll();
	        		break;
	        	}
            }
        }

        return true;
	}

	public void send(PaxosMessage aMessage, InetSocketAddress aTarget) {
		_transport.send(aMessage, aTarget);
	}
	
	public VoteOutcome getNext(long aTimeout) {
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
