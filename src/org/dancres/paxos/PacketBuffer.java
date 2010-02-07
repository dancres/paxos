package org.dancres.paxos;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.dancres.paxos.messages.PaxosMessage;

/**
 * PacketBuffer tracks a collection of PaxosMessages. Ordered by sequence number and then arrival time.
 */
class PacketBuffer {
	private SortedMap<Long, List<PaxosMessage>> _messagesByInstance = new TreeMap<Long, List<PaxosMessage>>();
	
	void add(PaxosMessage aMessage) {
		synchronized(this) {
			List<PaxosMessage> myInstance = _messagesByInstance.get(new Long(aMessage.getSeqNum()));
			
			if (myInstance == null) {
				myInstance = new ArrayList<PaxosMessage>();
				_messagesByInstance.put(new Long(aMessage.getSeqNum()), myInstance);
			}
			
			myInstance.add(aMessage);			
			notifyAll();
		}
	}
	
	PaxosMessage await(PaxosFilter aFilter) {
		synchronized(this) {
			while (true) {
				Iterator<List<PaxosMessage>> myInstances = _messagesByInstance.values().iterator();

				while (myInstances.hasNext()) {
					List<PaxosMessage> myInstance = myInstances.next();

					Iterator<PaxosMessage> myMessages = myInstance.iterator();

					while (myMessages.hasNext()) {
						PaxosMessage myMsg = myMessages.next();
						if (aFilter.interested(myMsg)) {
							myMessages.remove();
							return myMsg;
						}
					}
				}

				try {
					wait();
				} catch (InterruptedException anIE) {
					// Doesn't matter
				}
			}
		}
	}
	
	public interface PaxosFilter {
		public boolean interested(PaxosMessage aMessage);
	}
}
