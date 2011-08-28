package org.dancres.paxos.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dancres.paxos.messages.PaxosMessage;

/**
 * PacketBuffer tracks a collection of <code>PaxosMessage</code>s. Duplicates are eliminated and do not re-trigger
 * <code>PaxosFilter</code> instances.
 */
public class PacketBuffer {
	private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();
	
	public void add(PaxosMessage aMessage) {
		synchronized(this) {
			if (! _messages.contains(aMessage)) {
				_messages.add(aMessage);
				notifyAll();
			}
		}
	}
	
	public PaxosMessage await(PaxosFilter aFilter, long aWaitTime) {
		long myExpiry = System.currentTimeMillis() + aWaitTime;
		
		synchronized(this) {
			while (true) {
				Iterator<PaxosMessage> myMessages = _messages.iterator();

				while (myMessages.hasNext()) {
					PaxosMessage myMsg = myMessages.next();
					if (aFilter.interested(myMsg)) {
						myMessages.remove();
						return myMsg;
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
