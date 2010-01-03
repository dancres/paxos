package org.dancres.paxos;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;

/**
 * PacketBuffer tracks a collection of PaxosMessages. The collection is ordered by sequence number and secondarily
 * by order of arrival. 
 */
class PacketBuffer {
	private SortedMap<Long, List<PaxosMessage>> _packets = new TreeMap<Long, List<PaxosMessage>>();
	private long _capacity;
	
	PacketBuffer(long aCapacity) {
		_capacity = aCapacity;
	}
	
	void add(PaxosMessage aMessage) {
		synchronized(this) {
			List<PaxosMessage> myBuffer = _packets.get(new Long(aMessage.getSeqNum()));
			
			if (myBuffer == null) {
				if (_packets.size() == _capacity) {
					_packets.remove(_packets.firstKey());
				}
				
				myBuffer = new LinkedList<PaxosMessage>();
				myBuffer.add(aMessage);
				_packets.put(new Long(aMessage.getSeqNum()), myBuffer);
			} else {
				myBuffer.add(aMessage);
			}
		}
	}
	
	void dump(Logger aLogger) {
		synchronized(this) {
			Iterator<Long> mySeqs = _packets.keySet().iterator();
			while (mySeqs.hasNext()) {
				Long mySeq = mySeqs.next();
				
				Iterator<PaxosMessage> myPackets = _packets.get(mySeq).iterator();
				while (myPackets.hasNext()) {
					aLogger.info(myPackets.next().toString());
				}
			}
		}
	}
}
