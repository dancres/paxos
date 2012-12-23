package org.dancres.paxos.test.net;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;

public class ClientDispatcher implements Transport.Dispatcher {
	private Transport _transport;
	private List<VoteOutcome> _queue = new ArrayList<VoteOutcome>();
	
	public boolean messageReceived(Packet aPacket) {
		PaxosMessage myMessage = aPacket.getMessage();
		
		synchronized(this) {
	        switch (myMessage.getType()) {
	        	case Operations.EVENT : {
	        		_queue.add((VoteOutcome) myMessage);
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
	
	/**
	 * @param aTimeout the time in milliseconds to wait for the outcome of a negative number indicating no wait
	 */
	public VoteOutcome getNext(long aTimeout) {
		synchronized(this) {
			while (_queue.isEmpty()) {
				try {
					if (aTimeout >= 0)
						wait(aTimeout);
					else
						return null;
				} catch (InterruptedException anIE) {					
				}
			}
			
			return _queue.remove(0);
		}
	}
	
	public void setTransport(Transport aTransport) {
		_transport = aTransport;
	}
	
	public void terminate() {
	}
}
