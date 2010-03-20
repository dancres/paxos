package org.dancres.paxos;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.dancres.paxos.messages.Accept;
import org.dancres.paxos.messages.Ack;
import org.dancres.paxos.messages.Begin;
import org.dancres.paxos.messages.Collect;
import org.dancres.paxos.messages.Last;
import org.dancres.paxos.messages.Need;
import org.dancres.paxos.messages.OldRound;
import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.Success;
import org.dancres.paxos.messages.codec.Codecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the Acceptor/Learner state machine. Note that the instance running
 * in the same JVM as the current leader is to all intents and purposes (bar
 * very strange hardware or operating system failures) guaranteed to receive
 * packets from the leader. Thus if a leader declares SUCCESS then the local
 * instance will receive those packets. This can be useful for processing client
 * requests correctly and signalling interested parties as necessary.
 * 
 * @author dan
 */
public class AcceptorLearner {
	public static final ConsolidatedValue HEARTBEAT = new ConsolidatedValue(
			"org.dancres.paxos.Heartbeat".getBytes(), new byte[] {});

	private static final short UP_TO_DATE = 1;
	private static final short OUT_OF_DATE = 2;
	
	private static long DEFAULT_LEASE = 30 * 1000;
	private static Logger _logger = LoggerFactory
			.getLogger(AcceptorLearner.class);

	/**
	 * Statistic that tracks the number of Collects this AcceptorLearner ignored
	 * from competing leaders within DEFAULT_LEASE ms of activity from the
	 * current leader.
	 */
	private AtomicLong _ignoredCollects = new AtomicLong();
	private AtomicLong _receivedHeartbeats = new AtomicLong();

	private short _state = UP_TO_DATE;

	private static class RecoveryWindow {
		private long _minSeqNum;
		private long _maxSeqNum;
		
		RecoveryWindow(long aMin, long aMax) {
			_minSeqNum = aMin;
			_maxSeqNum = aMax;
		}
		
		long getMinSeqNum() {
			return _minSeqNum;
		}

		long getMaxSeqNum() {
			return _maxSeqNum;
		}
	}
		
	/**
	 * Tracks the range of sequence numbers we're interested in getting from some other AL.
	 */
	private RecoveryWindow _recoveryWindow;
	
	/**
	 * @todo Must checkpoint _lastCollect, as it'll only be written in the log
	 *       file the first time it appears and thus when we hit a checkpoint it
	 *       will be discarded. This is because we implement the optimisation
	 *       for multi-paxos described in "Paxos Made Simple" such that we
	 *       needn't sync to disk for a Collect that is identical to and comes
	 *       from the same Leader as for previous rounds.
	 */
	private Collect _lastCollect = Collect.INITIAL;
	private long _lastLeaderActionTime = 0;

	private LogStorage _storage;
	private Transport _transport;
	private FailureDetector _fd;
	
	private final long _leaderLease;
	private final List<AcceptorLearnerListener> _listeners = new ArrayList<AcceptorLearnerListener>();
	
	/**
	 * Tracks the last contiguous sequence number for which we have a value.
	 * 
	 * When we receive a success, if it's seqNum is this field + 1, increment
	 * this field. Acts as the low watermark for leader recovery, essentially we
	 * want to recover from the last contiguous sequence number in the stream of
	 * paxos instances.
	 */
	public static class Watermark {
		static final Watermark INITIAL = new Watermark(LogStorage.NO_SEQ, -1);
		private long _seqNum;
		private long _logOffset;
		
		private Watermark(long aSeqNum, long aLogOffset) {
			_seqNum = aSeqNum;
			_logOffset = aLogOffset;
		}
		
		public long getSeqNum() {
			return _seqNum;
		}
		
		public long getLogOffset() {
			return _logOffset;
		}
		
		public boolean equals(Object anObject) {
			if (anObject instanceof Watermark) {
				Watermark myOther = (Watermark) anObject;
				
				return (myOther._seqNum == _seqNum) && (myOther._logOffset == _logOffset);
			}
			
			return false;
		}
		
		public String toString() {
			return "Watermark: " + Long.toHexString(_seqNum) + ", " + Long.toHexString(_logOffset);
		}
	}

	private Watermark _lowSeqNumWatermark = Watermark.INITIAL;

	public AcceptorLearner(LogStorage aStore, FailureDetector anFD, Transport aTransport) {
		this(aStore, anFD, aTransport, DEFAULT_LEASE);
	}

	public AcceptorLearner(LogStorage aStore, FailureDetector anFD, Transport aTransport, long aLeaderLease) {
		_storage = aStore;
		_transport = aTransport;
		_fd = anFD;
		
		try {
			restore();
		} catch (Exception anE) {
			_logger.error("Failed to open logger", anE);
			throw new RuntimeException(anE);
		}
		
		_leaderLease = aLeaderLease;
	}
	
	/**
	 * @todo Replay state, reload checkpoint etc
	 * 
	 * @throws Exception
	 */
	private void restore() throws Exception {
		_storage.open();		
	}
	
	public void close() {
		try {
			_storage.close();
		} catch (Exception anE) {
			_logger.error("Failed to close logger", anE);
			throw new RuntimeException(anE);
		}
	}

	public long getLeaderLeaseDuration() {
		return _leaderLease;
	}

	public boolean isRecovering() {
		synchronized(this) {
			return (_state != UP_TO_DATE);
		}
	}
	
	public void add(AcceptorLearnerListener aListener) {
		synchronized (_listeners) {
			_listeners.add(aListener);
		}
	}

	public void remove(AcceptorLearnerListener aListener) {
		synchronized (_listeners) {
			_listeners.remove(aListener);
		}
	}

	public long getHeartbeatCount() {
		return _receivedHeartbeats.longValue();
	}

	public long getIgnoredCollectsCount() {
		return _ignoredCollects.longValue();
	}

	private LogStorage getStorage() {
		return _storage;
	}

	private void updateLowWatermark(long aSeqNum, long aLogOffset) {
		synchronized (this) {
			if (_lowSeqNumWatermark.getSeqNum() == (aSeqNum - 1)) {
				_lowSeqNumWatermark = new Watermark(aSeqNum, aLogOffset);

				_logger.info("AL:Low :" + _lowSeqNumWatermark);
			}
		}
	}

	public Watermark getLowWatermark() {
		synchronized (this) {
			return _lowSeqNumWatermark;
		}
	}

	public Collect getLastCollect() {
		synchronized (this) {
			return _lastCollect;
		}
	}

	/**
	 * @param aCollect
	 *            should be tested to see if it supercedes the current COLLECT
	 * @return <code>true</code> if it supercedes, <code>false</code> otherwise
	 */
	private boolean supercedes(Collect aCollect) {
		synchronized (this) {
			if (aCollect.supercedes(_lastCollect)) {
				_lastCollect = aCollect;

				return true;
			} else {
				return false;
			}
		}
	}

	private boolean originates(Begin aBegin) {
		synchronized (this) {
			return aBegin.originates(_lastCollect);
		}
	}

	private boolean precedes(Begin aBegin) {
		synchronized (this) {
			return aBegin.precedes(_lastCollect);
		}
	}

	/**
	 * @return <code>true</code> if the collect is either from the existing
	 *         leader, or there is no leader or there's been nothing heard from
	 *         the current leader within DEFAULT_LEASE milliseconds else
	 *         <code>false</code>
	 */
	private boolean amAccepting(Collect aCollect, long aCurrentTime) {
		synchronized (this) {
			if (_lastCollect.isInitial()) {
				return true;
			} else {
				if (isFromCurrentLeader(aCollect))
					return true;
				else
					return (aCurrentTime > _lastLeaderActionTime
							+ _leaderLease);
			}
		}
	}

	private boolean isFromCurrentLeader(Collect aCollect) {
		synchronized (this) {
			return aCollect.sameLeader(_lastCollect);
		}
	}

	private void updateLastActionTime(long aTime) {
		_logger.info("AL:Updating last action time: " + aTime);

		synchronized (this) {
			if (aTime > _lastLeaderActionTime)
				_lastLeaderActionTime = aTime;
		}
	}

	private static class Dispatch {
		static final Dispatch NO_RESPONSE = new Dispatch(0, null);
		
		private NodeId _nodeId;
		private PaxosMessage _message;
		
		Dispatch(long anId, PaxosMessage aMessage) {
			_nodeId = NodeId.from(anId);
			_message = aMessage;
		}
		
		Dispatch(NodeId anId, PaxosMessage aMessage) {
			_nodeId = anId;
			_message = aMessage;
		}
		
		PaxosMessage getMessage() {
			return _message;
		}
		
		NodeId getNodeId() {
			return _nodeId;
		}
	}
	
	public void messageReceived(PaxosMessage aMessage) {
		Dispatch myDispatch;
		short myState;
		
		synchronized(this) {
			myState = _state;
		}
		
		if (myState != UP_TO_DATE) {
			myDispatch = recover(aMessage);
		} else {		
			myDispatch = process(aMessage);
		}
		
		if (! myDispatch.equals(Dispatch.NO_RESPONSE)) {
			_transport.send(myDispatch.getMessage(), myDispatch.getNodeId());
		}
	}
	
	/**
	 * @todo Implement recovery
	 * 
	 * @param aMessage is the message to process
	 * @return is any message to send
	 */
	private Dispatch recover(PaxosMessage aMessage) {
		if (aMessage.getClassification() == PaxosMessage.LEADER)
			/* 
			 * Standard protocol messages are discarded unless they're for sequence numbers greater than our recovery
			 * window.
			 */
			return Dispatch.NO_RESPONSE;
		else {
			// Switch statement and test
			return Dispatch.NO_RESPONSE;
		}
	}
	
	/**
	 * @todo Implement recovery protocol packets etc
	 * 
	 * @param aMessage is the message to process
	 * @return is any message to send
	 */
	private Dispatch process(PaxosMessage aMessage) {
		long myNodeId = aMessage.getNodeId();
		long myCurrentTime = System.currentTimeMillis();
		long mySeqNum = aMessage.getSeqNum();

		_logger.info("AL: got [ " + mySeqNum + " ] : " + aMessage);

		/*
		 * If the sequence number we're seeing is for a sequence number > lwm + 1, we've missed some packets.
		 * Recovery range r is lwm < r < x (where x = currentMessage.seqNum + 1) 
		 * so that the round we're seeing right now is complete and we need save packets only after that point.
		 */
		if (mySeqNum > getLowWatermark().getSeqNum() + 1) {
			synchronized(this) {
				_state = OUT_OF_DATE;
				_recoveryWindow = new RecoveryWindow(getLowWatermark().getSeqNum(), mySeqNum + 1);

				// Return a broadcast message for AL with messages we require
				//
				return new Dispatch(NodeId.BROADCAST, 
						new Need(_recoveryWindow.getMinSeqNum(), _recoveryWindow.getMaxSeqNum(), 
								_transport.getLocalNodeId().asLong()));
			}
		}
		
		switch (aMessage.getType()) {
			case Operations.COLLECT: {
				Collect myCollect = (Collect) aMessage;

				if (!amAccepting(myCollect, myCurrentTime)) {
					_ignoredCollects.incrementAndGet();

					_logger.info("AL:Not accepting: " + myCollect + ", "
							+ getIgnoredCollectsCount());
					return Dispatch.NO_RESPONSE;
				}

				// If the collect supercedes our previous collect save it to disk,
				// return last proposal etc
				//
				if (supercedes(myCollect)) {
					write(aMessage, true);
					return new Dispatch(myNodeId, constructLast(mySeqNum));

					/*
					 * If the collect comes from the current leader (has same rnd
					 * and node), we apply the multi-paxos optimisation, no need to
					 * save to disk, just respond with last proposal etc
					 */
				} else if (isFromCurrentLeader(myCollect)) {
					return new Dispatch(myNodeId, constructLast(mySeqNum));

				} else {
					// Another collect has already arrived with a higher priority,
					// tell the proposer it has competition
					//
					Collect myLastCollect = getLastCollect();

					return new Dispatch(myNodeId, new OldRound(mySeqNum, myLastCollect.getNodeId(),
							myLastCollect.getRndNumber(), _transport.getLocalNodeId().asLong()));
				}
			}

			case Operations.BEGIN: {
				Begin myBegin = (Begin) aMessage;

				// If the begin matches the last round of a collect we're fine
				//
				if (originates(myBegin)) {
					updateLastActionTime(myCurrentTime);
					write(aMessage, true);

					return new Dispatch(myNodeId, 
							new Accept(mySeqNum, getLastCollect().getRndNumber(), 
									_transport.getLocalNodeId().asLong()));
				} else if (precedes(myBegin)) {
					// New collect was received since the collect for this begin,
					// tell the proposer it's got competition
					//
					Collect myLastCollect = getLastCollect();

					return new Dispatch(myNodeId, new OldRound(mySeqNum, myLastCollect.getNodeId(),
							myLastCollect.getRndNumber(), _transport.getLocalNodeId().asLong()));
				} else {
					// Quiet, didn't see the collect, leader hasn't accounted for
					// our values, it hasn't seen our last
					//
					_logger.info("AL:Missed collect, going silent: " + mySeqNum
							+ " [ " + myBegin.getRndNumber() + " ]");
					return Dispatch.NO_RESPONSE;
				}
			}

			case Operations.SUCCESS: {
				Success mySuccess = (Success) aMessage;

				updateLastActionTime(myCurrentTime);

				if (mySuccess.getSeqNum() <= getLowWatermark().getSeqNum()) {
					_logger.info("AL:Discarded known value: " + mySuccess.getSeqNum());
					return new Dispatch(myNodeId, new Ack(mySuccess.getSeqNum(), 
							_transport.getLocalNodeId().asLong()));
				} else
					_logger.info("AL:Learnt value: " + mySuccess.getSeqNum());

				// Always record the value even if it's the heartbeat so there are
				// no gaps in the Paxos sequence
				//
				long myLogOffset = write(aMessage, true);

				updateLowWatermark(mySuccess.getSeqNum(), myLogOffset);

				if (mySuccess.getConsolidatedValue().equals(HEARTBEAT)) {
					_receivedHeartbeats.incrementAndGet();

					_logger.info("AL: discarded heartbeat: "
							+ System.currentTimeMillis() + ", "
							+ getHeartbeatCount());
				} else {
					signal(new Event(Event.Reason.DECISION, mySuccess.getSeqNum(),
							mySuccess.getConsolidatedValue(), null));
				}

				return new Dispatch(myNodeId, new Ack(mySuccess.getSeqNum(), _transport.getLocalNodeId().asLong()));
			}

			default:
				throw new RuntimeException("Unexpected message");
		}
	}

	private static class InstanceState {
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
	}
	
	/**
	 * @todo When we add checkpointing, we must add support here for warning a leader it is out of date and asking
	 * about a sequence number that is no longer in the log
	 */
	private Last constructLast(long aSeqNum) {
		Watermark myLow = getLowWatermark();
		
		/*
		 * If the sequence number is less than the current low watermark, we've got to check through the log file for
		 * the value otherwise if it's present, it will be since the low watermark offset.
		 */
		InstanceState myState;
		
		if ((myLow.equals(Watermark.INITIAL)) || (aSeqNum <= myLow.getSeqNum())) {
			myState = scanLog(aSeqNum, 0);
		} else 
			myState = scanLog(aSeqNum, myLow.getLogOffset());
		
		if ((myState == null) || (myState.getLastValue() == null)) {
			return new Last(aSeqNum, myLow.getSeqNum(), Long.MIN_VALUE,
					LogStorage.NO_VALUE, _transport.getLocalNodeId().asLong());
		} else {			
			Begin myBegin = myState.getLastValue();
			
			return new Last(aSeqNum, myLow.getSeqNum(), myBegin.getRndNumber(), myBegin.getConsolidatedValue(),
					_transport.getLocalNodeId().asLong());
		}
	}

	private long write(PaxosMessage aMessage, boolean aForceRequired) {
		long myLogOffset;
		
		try {
			myLogOffset = getStorage().put(Codecs.encode(aMessage), aForceRequired); 
			
			// dump("Writing @ " + Long.toHexString(myLogOffset) + " : ", Codecs.encode(aMessage));
			
			return myLogOffset;
		} catch (Exception anE) {
			_logger.error("AL: cannot log: " + System.currentTimeMillis(), anE);
			throw new RuntimeException(anE);
		}
	}

	private static class ReplayListenerImpl implements RecordListener {
		private long _seqNum;
		private InstanceState _state = null;
		
		ReplayListenerImpl(long aSeqNum) {
			_seqNum = aSeqNum;
		}
		
		InstanceState getState() {
			return _state;
		}
		
		public void onRecord(long anOffset, byte[] aRecord) {
			// dump("Read:", aRecord);
			
			// All records we write are leader messages and they all have no length
			//
			PaxosMessage myMessage = Codecs.decode(aRecord);

			if (myMessage.getSeqNum() == _seqNum) {
				if (_state == null)
					_state = new InstanceState(_seqNum);
				
				_state.add(myMessage, anOffset);
			}
		}		
	}
	
	private InstanceState scanLog(long aSeqNum, long aLogOffset) {
		try {
			ReplayListenerImpl myListener = new ReplayListenerImpl(aSeqNum);
			_storage.replay(myListener, aLogOffset);
			
			return myListener.getState();
		} catch (Exception anE) {
			_logger.error("Failed to replay log", anE);
			throw new RuntimeException("Failed to replay log", anE);
		}
	}
	
	void signal(Event aStatus) {
		List<AcceptorLearnerListener> myListeners;

		synchronized (_listeners) {
			myListeners = new ArrayList<AcceptorLearnerListener>(_listeners);
		}

		Iterator<AcceptorLearnerListener> myTargets = myListeners.iterator();

		while (myTargets.hasNext()) {
			myTargets.next().done(aStatus);
		}
	}
	
    private static void dump(String aMessage, byte[] aBuffer) {
    	System.err.print(aMessage + " ");
    	
        for (int i = 0; i < aBuffer.length; i++) {
            System.err.print(Integer.toHexString(aBuffer[i]) + " ");
        }

        System.err.println();
    }	
}
