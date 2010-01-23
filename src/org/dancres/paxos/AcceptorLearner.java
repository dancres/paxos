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
 * @todo Checkpoint must include low watermark etc.
 * 
 * @todo Recovery: At construction, AL should load whatever state it has locally on disk. 
 * 
 * Recovery: Past that it waits for further updates which may then cause it to trigger further recovery. 
 * e.g. Because low watermark and current sequence number are too far apart. It may trigger several different kinds of
 * recovery based on what it sees, e.g. quick catchup from other nodes' memory or a full file catchup. Whichever method 
 * it uses, it's important it stops the leader acting on its out of date state. As the leader will always ask for the
 * low watermark and what the last viewed collect was when it initiates action, we can simply raise an exception to
 * cause the leader to fail and issue an appropriate response to clients. It would be nice to use the log as a means
 * for recalling old values etc. To do this requires that we only checkpoint upto and including the log entry of the low
 * watermark we cannot checkpoint higher as the values may not have settled out yet and we need to maintain references
 * in the log. When we checkpoint, we must note the lowest paxos instance the log will contain - which is 
 * low watermark + 1 (We could of course just note the low watermark itself) so that if a node asks for recovery of
 * sequence numbers that aren't in the log can be spotted and informed of their being too out of date.
 * 
 * The ordered delivery of events to user-code and the problem of a lagging low watermark (when we end up dealing in
 * sequence numbers that aren't contiguous perhaps as the result of temporary network separation) are closely related.
 * We can address both via the PacketBuffer as follows:
 * 
 * <ol>
 * <li>Each instance of Paxos is added into the buffer which tracks them in sequence number order.</li>
 * <li>At the point where we reach success, we attempt an update of the low watermark.</li>
 * <li>We check to see if the head of the buffer (which will include the Paxos instance just completed) is current
 * low watermark + 1 and if it is increment the watermark and emit the value in the success record to the 
 * user-code.</li>
 * <li>We then repeat this check until either the buffer is empty or we encounter an instance which has sequence number
 * > current low watermark + 1.</li>
 * </ol>
 * 
 * @author dan
 */
public class AcceptorLearner {
	public static final ConsolidatedValue HEARTBEAT = new ConsolidatedValue(
			"org.dancres.paxos.Heartbeat".getBytes(), new byte[] {});

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

	/**
	 * PacketBuffer is used to maintain a limited amount of past Paxos history
	 * that can be used to catch-up recovering nodes without the cost of a full
	 * transfer of logs etc. This is useful in cases where nodes temporarily
	 * fail due to loss of a network connection or reboots.
	 */
	private PacketBuffer _buffer = new PacketBuffer(512);

	private final List<AcceptorLearnerListener> _listeners = new ArrayList<AcceptorLearnerListener>();

	public AcceptorLearner(LogStorage aStore) {
		_storage = aStore;

		try {
			restore();
		} catch (Exception anE) {
			_logger.error("Failed to open logger", anE);
			throw new RuntimeException(anE);
		}
	}

	private void restore() throws Exception {
		_storage.open();		
	}
	
	public void close() {
		try {
			_storage.close();
			_buffer.dump(_logger);
		} catch (Exception anE) {
			_logger.error("Failed to close logger", anE);
			throw new RuntimeException(anE);
		}
	}

	public long getLeaderLeaseDuration() {
		return DEFAULT_LEASE;
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
				Collect myOld = _lastCollect;
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
							+ DEFAULT_LEASE);
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

	/**
	 * @todo FIX THIS - we need to return a value in a LAST not just a default!
	 */
	public PaxosMessage process(PaxosMessage aMessage) {
		long myCurrentTime = System.currentTimeMillis();
		long mySeqNum = aMessage.getSeqNum();

		_logger.info("AL: got [ " + mySeqNum + " ] : " + aMessage);

		switch (aMessage.getType()) {
			case Operations.COLLECT: {
				Collect myCollect = (Collect) aMessage;

				if (!amAccepting(myCollect, myCurrentTime)) {
					_ignoredCollects.incrementAndGet();

					_logger.info("AL:Not accepting: " + myCollect + ", "
							+ getIgnoredCollectsCount());
					return null;
				}

				// If the collect supercedes our previous collect sace it to disk,
				// return last proposal etc
				//
				if (supercedes(myCollect)) {
					write(aMessage, true);
					return constructLast(mySeqNum);

					/*
					 * If the collect comes from the current leader (has same rnd
					 * and node), we apply the multi-paxos optimisation, no need to
					 * save to disk, just respond with last proposal etc
					 */
				} else if (isFromCurrentLeader(myCollect)) {
					return constructLast(mySeqNum);

				} else {
					// Another collect has already arrived with a higher priority,
					// tell the proposer it has competition
					//
					Collect myLastCollect = getLastCollect();

					return new OldRound(mySeqNum, myLastCollect.getNodeId(),
							myLastCollect.getRndNumber());
				}
			}

			case Operations.BEGIN: {
				Begin myBegin = (Begin) aMessage;

				// If the begin matches the last round of a collect we're fine
				//
				if (originates(myBegin)) {
					updateLastActionTime(myCurrentTime);
					write(aMessage, true);

					return new Accept(mySeqNum, getLastCollect().getRndNumber());
				} else if (precedes(myBegin)) {
					// New collect was received since the collect for this begin,
					// tell the proposer it's got competition
					//
					Collect myLastCollect = getLastCollect();

					return new OldRound(mySeqNum, myLastCollect.getNodeId(),
							myLastCollect.getRndNumber());
				} else {
					// Quiet, didn't see the collect, leader hasn't accounted for
					// our values, it hasn't seen our last
					//
					_logger.info("AL:Missed collect, going silent: " + mySeqNum
							+ " [ " + myBegin.getRndNumber() + " ]");
				}
			}

			case Operations.SUCCESS: {
				Success mySuccess = (Success) aMessage;

				updateLastActionTime(myCurrentTime);

				if (mySuccess.getSeqNum() <= getLowWatermark().getSeqNum()) {
					_logger.info("AL:Discarded known value: " + mySuccess.getSeqNum());
					return null;
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

				return new Ack(mySuccess.getSeqNum());
			}

			default:
				throw new RuntimeException("Unexpected message");
		}
	}

	private Last constructLast(long aSeqNum) {
		Begin myLastBegin = _buffer.getLastBegin(aSeqNum);

		if (myLastBegin != null)
			return new Last(aSeqNum, getLowWatermark().getSeqNum(), myLastBegin
					.getRndNumber(), myLastBegin.getConsolidatedValue());
		else
			return new Last(aSeqNum, getLowWatermark().getSeqNum(), Long.MIN_VALUE,
					LogStorage.NO_VALUE);
	}

	private long write(PaxosMessage aMessage, boolean aForceRequired) {
		long myLogOffset;
		
		try {
			myLogOffset = getStorage().put(Codecs.encode(aMessage), aForceRequired); 
			_buffer.add(aMessage, myLogOffset);
			
			return myLogOffset;
		} catch (Exception anE) {
			_logger.error("AL: cannot log: " + System.currentTimeMillis(), anE);
			throw new RuntimeException(anE);
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
}
