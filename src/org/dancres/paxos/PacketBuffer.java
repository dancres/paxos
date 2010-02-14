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
public class PacketBuffer {
	private SortedMap<Long, List<PaxosMessage>> _messagesByInstance = new TreeMap<Long, List<PaxosMessage>>();
	
	public void add(PaxosMessage aMessage) {
		synchronized(this) {
			List<PaxosMessage> myInstance = _messagesByInstance.get(new Long(aMessage.getSeqNum()));
			
			if (myInstance == null) {
				myInstance = new ArrayList<PaxosMessage>();
				_messagesByInstance.put(new Long(aMessage.getSeqNum()), myInstance);
			}
			
			if (! myInstance.contains(aMessage)) {
				myInstance.add(aMessage);			
				notifyAll();
			}
		}
	}
	
	public PaxosMessage await(PaxosFilter aFilter, long aWaitTime) {
		long myExpiry = System.currentTimeMillis() + aWaitTime;
		
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
					if (aWaitTime == 0) {
						wait(0);
					} else {
						long myActualWait = myExpiry - System.currentTimeMillis();
												
						if (myActualWait > 0)
							wait(myActualWait);
						else
							return null;
					}
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
