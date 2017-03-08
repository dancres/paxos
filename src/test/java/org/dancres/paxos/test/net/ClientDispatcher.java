package org.dancres.paxos.test.net;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.impl.Transport.Packet;
import org.dancres.paxos.messages.Event;
import org.dancres.paxos.messages.PaxosMessage;

public class ClientDispatcher implements Transport.Dispatcher {
	private Transport _transport;
	private List<VoteOutcome> _queue = new ArrayList<>();
	
	public boolean packetReceived(Packet aPacket) {
		PaxosMessage myMessage = aPacket.getMessage();
		
		synchronized(this) {
	        switch (myMessage.getType()) {
	        	case PaxosMessage.Types.EVENT : {
                    Event myEvent = (Event) myMessage;
	        		_queue.add(myEvent.getOutcome());
	        		notifyAll();
	        		break;
	        	}
            }
        }

        return true;
	}

	public void send(PaxosMessage aMessage, InetSocketAddress aTarget) {
		_transport.send(_transport.getPickler().newPacket(aMessage), aTarget);
	}
	
	/**
	 * @param aTimeout the time in milliseconds to wait for the outcome of a negative number indicating no wait
	 */
	public VoteOutcome getNext(long aTimeout) {
		long myStartTime = System.currentTimeMillis();

		synchronized(this) {
			while (_queue.isEmpty()) {
				try {
					long myCurrent = System.currentTimeMillis();

					if ((aTimeout >= 0) && (myCurrent < myStartTime + aTimeout)) {
							wait(myStartTime + aTimeout - myCurrent);
					} else {
						return null;
					}
				} catch (InterruptedException anIE) {
				}
			}
			
			return _queue.remove(0);
		}
	}
	
	public void init(Transport aTransport) {
		_transport = aTransport;
	}
	
	public void terminate() {
	}
}
