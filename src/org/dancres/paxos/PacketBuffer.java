package org.dancres.paxos;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Success;
import org.slf4j.Logger;

/**
 * PacketBuffer tracks a collection of PaxosMessages. The collection is ordered by sequence number and secondarily
 * by operation type. PacketBuffer also tracks log offset for each message which is used to compute appropriate
 * checkpoints in log files.
 */
class PacketBuffer {
	private static class InstanceState {
		private Begin _lastBegin;
		private long _lastBeginOffset;
		
		private Success _lastSuccess;
		private long _lastSuccessOffset;
		
		private long _seqNum;
		
		InstanceState(long aSeqNum) {
			_seqNum = aSeqNum;
		}
		
		void add(PaxosMessage aMessage, long aLogOffset) {
			switch(aMessage.getType()) {
				case Operations.COLLECT : {
					// Nothing to do
					//
					break;
				}
				
				case Operations.BEGIN : {
					
					Begin myBegin = (Begin) aMessage;
					
					if (_lastBegin == null) {
						_lastBegin = myBegin;
						_lastBeginOffset = aLogOffset;
					} else if (myBegin.getRndNumber() > _lastBegin.getRndNumber() ){
						_lastBegin = myBegin;
						_lastBeginOffset = aLogOffset;
					}
					
					break;
				}
				
				case Operations.SUCCESS : {
					
					Success myLastSuccess = (Success) aMessage;

					_lastSuccess = myLastSuccess;
					_lastSuccessOffset = aLogOffset;
					
					break;
				}
				
				default : throw new RuntimeException("Unexpected message: " + aMessage);
			}
		}

		public Begin getLastBegin() {
			return _lastBegin;
		}
		
		public String toString() {
			return "LoggedInstance: " + _seqNum + " " + _lastBegin + " @ " + _lastBeginOffset +
				" " + _lastSuccess + " @ " + _lastSuccessOffset;
		}
	}
	
	private SortedMap<Long, InstanceState> _packets = new TreeMap<Long, InstanceState>();
	private long _capacity;
	
	PacketBuffer(long aCapacity) {
		_capacity = aCapacity;
	}
	
	void add(PaxosMessage aMessage, long aLogOffset) {
		synchronized(this) {
			InstanceState myState = _packets.get(new Long(aMessage.getSeqNum()));
			
			if (myState == null) {
				if (_packets.size() == _capacity) {
					_packets.remove(_packets.firstKey());
				}
				
				myState = new InstanceState(aMessage.getSeqNum());
				myState.add(aMessage, aLogOffset);
				_packets.put(new Long(aMessage.getSeqNum()), myState);
			} else {
				myState.add(aMessage, aLogOffset);
			}
		}
	}
	
	void dump(Logger aLogger) {
		synchronized(this) {
			Iterator<InstanceState> mySeqs = _packets.values().iterator();
			
			while (mySeqs.hasNext()) {
				aLogger.info(mySeqs.next().toString());
			}
		}
	}

	Begin getLastBegin(long aSeqNum) {
		synchronized(this) {
			InstanceState myState = _packets.get(new Long(aSeqNum));
			
			Begin myBegin = null;
			
			if (myState != null) {
				return myState.getLastBegin();
			}
				
			return myBegin;
		}
	}
}
