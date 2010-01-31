package org.dancres.paxos;

import java.util.Iterator;
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
	static class InstanceState {
		private Begin _lastBegin;
		private long _lastBeginOffset;
		
		private Success _lastSuccess;
		private long _lastSuccessOffset;
		
		private long _seqNum;
		
		InstanceState(long aSeqNum) {
			_seqNum = aSeqNum;
		}
		
		synchronized void add(PaxosMessage aMessage, long aLogOffset) {
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

		synchronized Begin getLastValue() {
			if (_lastSuccess != null) {
				return new Begin(_lastSuccess.getSeqNum(), _lastSuccess.getRndNum(), 
						_lastSuccess.getConsolidatedValue(), _lastSuccess.getNodeId());
			} else if (_lastBegin != null) {
				return _lastBegin;
			} else {
				return null;
			}
		}
		
		public String toString() {
			return "LoggedInstance: " + _seqNum + " " + _lastBegin + " @ " + Long.toHexString(_lastBeginOffset) +
				" " + _lastSuccess + " @ " + Long.toHexString(_lastSuccessOffset);
		}

		public long getSeqNum() {
			return _seqNum;
		}
	}
	
	private SortedMap<Long, InstanceState> _packets = new TreeMap<Long, InstanceState>();
	
	PacketBuffer() {
	}
	
	void add(PaxosMessage aMessage, long aLogOffset) {
		/*
		 *  Ignore COLLECT - don't want to retain a state entry unless there was a genuine proposal as opposed to
		 *  a leadership election (which is all collect really does)
		 */
		if (aMessage.getType() == Operations.COLLECT)
			return;
		
		synchronized(this) {
			InstanceState myState = _packets.get(new Long(aMessage.getSeqNum()));
			
			if (myState == null) {
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

	InstanceState getState(long aSeqNum) {
		synchronized(this) {
			return _packets.get(new Long(aSeqNum));
		}		
	}
}
