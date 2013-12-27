package org.dancres.paxos.impl;

import org.dancres.paxos.*;
import org.dancres.paxos.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implements the Acceptor/Learner state machine. Note that the instance running
 * in the same JVM as the current leader is to all intents and purposes (bar
 * very strange hardware or operating system failures) guaranteed to receive
 * packets from the leader. Thus if a leader declares LEARNED then the local
 * instance will receive those packets. This can be useful for processing client
 * requests correctly and signalling interested parties as necessary.
 *
 * @author dan
 */
public class AcceptorLearner implements MessageProcessor {
    private static final long DEFAULT_RECOVERY_GRACE_PERIOD = 5 * 1000;

	private static final Logger _logger = LoggerFactory.getLogger(AcceptorLearner.class);

	/**
	 * Statistic that tracks the number of Collects this AcceptorLearner ignored
	 * from competing leaders within DEFAULT_LEASE ms of activity from the
	 * current leader.
	 */
	private final AtomicLong _ignoredCollects = new AtomicLong();
	private final AtomicLong _receivedHeartbeats = new AtomicLong();

    private final AtomicLong _gracePeriod = new AtomicLong(DEFAULT_RECOVERY_GRACE_PERIOD);

    private final AtomicReference<TimerTask> _recoveryAlarm = new AtomicReference<>(null);
    private final AtomicReference<Need> _recoveryWindow = new AtomicReference<>(null);

    private final List<Listener> _listeners = new CopyOnWriteArrayList<>();

	private final LogStorage _storage;
    private final Common _common;
    private final PacketSorter _sorter = new PacketSorter();

    /**
     * Begins contain the values being proposed. These values must be remembered from round to round of an instance
     * until it has been committed. For any instance we believe is unresolved we keep the last value proposed (from
     * the last acceptable Begin) cached (the begins are also logged to disk) such that when have "learned" for an
     * instance we remove the value from the cache and send it to any listeners. This saves us having to disk scans
     * for values and ensures that any value is logged only once (because it doesn't appear in any other messages).
     */
    private final Map<Long, Begin> _cachedBegins = new ConcurrentHashMap<>();
    private final Map<Long, AcceptLedger> _acceptLedgers = new ConcurrentHashMap<>();

    private final Lock _guardLock = new ReentrantLock();
    private final Condition _notActive = _guardLock.newCondition();
    private int _activeCount;

    /**
     * Tracks the last contiguous sequence number for which we have a value.
     *
     * When we receive a success, if it's seqNum is this field + 1, increment
     * this field. Acts as the low watermark for leader recovery, essentially we
     * want to recover from the last contiguous sequence number in the stream of
     * paxos instances.
     */
    public static class Watermark implements Comparable<Watermark> {
        static final Watermark INITIAL = new Watermark(Constants.UNKNOWN_SEQ, -1);
        private final long _seqNum;
        private final long _logOffset;

        /**
         * @param aSeqNum the current sequence
         * @param aLogOffset the log offset of the success record for this sequence number
         */
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

        public int compareTo(Watermark aWatermark) {
        	if (aWatermark.getSeqNum() == _seqNum)
        		return 0;
        	else if (aWatermark.getSeqNum() < _seqNum)
        		return 1;
        	else
        		return -1;
        }
        
        public String toString() {
            return "Watermark: " + Long.toHexString(_seqNum) + ", " + Long.toHexString(_logOffset);
        }
    }

    static class ALCheckpointHandle extends CheckpointHandle {    	
        private transient Watermark _lowWatermark;
        private transient Transport.Packet _lastCollect;
        private final transient AtomicReference<AcceptorLearner> _al = new AtomicReference<>(null);
        private transient Transport.PacketPickler _pr;

        ALCheckpointHandle(Watermark aLowWatermark, Transport.Packet aCollect, AcceptorLearner anAl,
                           Transport.PacketPickler aPickler) {
            _lowWatermark = aLowWatermark;
            _lastCollect = aCollect;
            _al.set(anAl);
            _pr = aPickler;
        }

        private void readObject(ObjectInputStream aStream) throws IOException, ClassNotFoundException {
            aStream.defaultReadObject();
            _lowWatermark = new Watermark(aStream.readLong(), aStream.readLong());
            _pr = (Transport.PacketPickler) aStream.readObject();

            byte[] myBytes = new byte[aStream.readInt()];
            aStream.readFully(myBytes);
            _lastCollect = _pr.unpickle(myBytes);
        }

        private void writeObject(ObjectOutputStream aStream) throws IOException {
            aStream.defaultWriteObject();
            aStream.writeLong(_lowWatermark.getSeqNum());
            aStream.writeLong(_lowWatermark.getLogOffset());
            aStream.writeObject(_pr);

            byte[] myCollect = _pr.pickle(_lastCollect);
            aStream.writeInt(myCollect.length);
            aStream.write(myCollect);
        }

        public void saved() throws Exception {
            AcceptorLearner myAL = _al.get();
            
            if (myAL == null)
                throw new IOException("Checkpoint already saved");

            if (_al.compareAndSet(myAL, null))
                myAL.saved(this);
        }

        public boolean isNewerThan(CheckpointHandle aHandle) {
            if (aHandle.equals(CheckpointHandle.NO_CHECKPOINT))
                return true;
            else if (aHandle instanceof ALCheckpointHandle) {
                ALCheckpointHandle myOther = (ALCheckpointHandle) aHandle;

                return (_lowWatermark.getSeqNum() > myOther._lowWatermark.getSeqNum());
            } else {
                throw new IllegalArgumentException("Where did you get this checkpoint from?");
            }
        }

        Watermark getLowWatermark() {
            return _lowWatermark;
        }

        Transport.Packet getLastCollect() {
            return _lastCollect;
        }
        
        public boolean equals(Object anObject) {
        	if (anObject instanceof ALCheckpointHandle) {
        		ALCheckpointHandle myOther = (ALCheckpointHandle) anObject;
        		
        		return ((_lowWatermark.equals(myOther._lowWatermark)) && 
                    (_lastCollect.getSource().equals(myOther._lastCollect.getSource())) &&
                    (_lastCollect.getMessage().equals(myOther._lastCollect.getMessage())));
        	}
        	
        	return false;
        }
    }
	
	private final AtomicReference<ALCheckpointHandle> _lastCheckpoint = new AtomicReference<>();
	
    /* ********************************************************************************************
     *
     * Lifecycle, checkpointing and open/close
     *
     ******************************************************************************************** */

    AcceptorLearner(LogStorage aStore, Common aCommon, Listener anInitialListener) {
        _storage = aStore;
        _common = aCommon;
        _listeners.add(anInitialListener);
    }

    private boolean guard() {
        _guardLock.lock();

        try {
            if (_common.getNodeState().test(NodeState.State.SHUTDOWN))
                return true;

            _activeCount++;
            return false;
        } finally {
            _guardLock.unlock();
        }
    }

    private void unguard() {
        _guardLock.lock();

        try {
            assert(_activeCount > 0);

            --_activeCount;
            _notActive.signal();
        } finally {
            _guardLock.unlock();
        }
    }

    /**
     * If this method fails be sure to invoke close regardless.
     *
     * @param aHandle
     * @throws Exception
     */
    public void open(CheckpointHandle aHandle) throws Exception {
        _lastCheckpoint.set(
                new ALCheckpointHandle(Watermark.INITIAL, _common.getLastCollect(),
                        null, _common.getTransport().getPickler()));

        _storage.open();

        try {
            _common.getNodeState().set(NodeState.State.RECOVERING);

            long myStartSeqNum = -1;
            
            if (! aHandle.equals(CheckpointHandle.NO_CHECKPOINT)) {
                if (aHandle instanceof ALCheckpointHandle) {
                    testAndSetCheckpoint((ALCheckpointHandle) aHandle);
                    myStartSeqNum = installCheckpoint((ALCheckpointHandle) aHandle);
                } else
                    throw new IllegalArgumentException("Not a valid CheckpointHandle: " + aHandle);
            }

            try {
                new LogRangeProducer(myStartSeqNum, Long.MAX_VALUE, new Consumer() {
                    public void process(Transport.Packet aPacket, long aLogOffset) {
                        AcceptorLearner.this.process(aPacket, new ReplayWriter(aLogOffset), new RecoverySender());
                    }
                }, _storage, _common.getTransport().getPickler()).produce(0);
            } catch (Exception anE) {
                _logger.error(toString() + " Failed to replay log", anE);
            }
        } finally {
            _common.getNodeState().testAndSet(NodeState.State.RECOVERING, NodeState.State.ACTIVE);
        }
    }

    public void close() {
        try {
            /*
             * Allow for the fact we might be actively processing packets. Mark ourselves shutdown and drain...
             */
            _common.getNodeState().set(NodeState.State.SHUTDOWN);

            _guardLock.lock();

            try {
                while (_activeCount != 0)
                    _notActive.await();
            } finally {
                _guardLock.unlock();
            }

            TimerTask myAlarm = _recoveryAlarm.getAndSet(null);
            if (myAlarm != null)
                myAlarm.cancel();

            _recoveryWindow.set(null);

            _common.clearLeadership();
            _common.install(Watermark.INITIAL);

            _sorter.clear();
            _cachedBegins.clear();

            _storage.close();

        } catch (Exception anE) {
            _logger.error(toString() + " Failed to close storage", anE);
            throw new Error(anE);
        }
    }

    public CheckpointHandle newCheckpoint() {
        if (guard())
            throw new IllegalStateException("Instance is shutdown");

        if (_common.getNodeState().test(NodeState.State.OUT_OF_DATE)) {
            unguard();

            throw new IllegalStateException("Instance is out of date");
        }

        // Low watermark will always lag collect so no need for atomicity here
        //
        try {
            return new ALCheckpointHandle(_common.getLowWatermark(),
                    _common.getLastCollect(), this, _common.getTransport().getPickler());
        } finally {
            unguard();
        }
    }

    private void saved(ALCheckpointHandle aHandle) throws Exception {
        if (guard())
            throw new IllegalStateException("Instance is shutdown");

        try {
            if (testAndSetCheckpoint(aHandle))
                _storage.mark(aHandle.getLowWatermark().getLogOffset(), true);
        } finally {
            unguard();
        }
    }

    private boolean testAndSetCheckpoint(ALCheckpointHandle aHandle) {
        // If the checkpoint we're installing is newer...
        //
        ALCheckpointHandle myCurrent;
        do {
            myCurrent = _lastCheckpoint.get();

            if (! aHandle.isNewerThan(myCurrent))
                return false;

        } while (! _lastCheckpoint.compareAndSet(myCurrent, aHandle));

        return true;
    }

    private long installCheckpoint(ALCheckpointHandle aHandle) {
        _common.setLastCollect(aHandle.getLastCollect());

        _logger.info(toString() + " Checkpoint installed: " + aHandle.getLastCollect() + " @ " +
                aHandle.getLowWatermark());

        return _common.install(aHandle.getLowWatermark());
    }

    /**
     * When an AL is out of date, call this method to bring it back into sync from a remotely sourced
     * checkpoint.
     *
     * @param aHandle obtained from the remote checkpoint.
     * @throws Exception
     */
    boolean bringUpToDate(CheckpointHandle aHandle) throws Exception {
        if (guard())
            throw new IllegalStateException("Instance is shutdown");

        try {
            if (! _common.getNodeState().test(NodeState.State.OUT_OF_DATE))
                throw new IllegalStateException("Not out of date");

            if (! (aHandle instanceof ALCheckpointHandle))
                throw new IllegalArgumentException("Not a valid CheckpointHandle: " + aHandle);

            ALCheckpointHandle myHandle = (ALCheckpointHandle) aHandle;

            if (! testAndSetCheckpoint(myHandle))
                return false;

            /*
             * If we're out of date, there will be no active timers and no activity on the log.
             * We should install the checkpoint and clear out all state associated with known instances
             * of Paxos. Additional state will be obtained from another AL via the lightweight recovery protocol.
             * Out of date means that the existing log has no useful data within it implying it can be discarded.
             */
            if (_common.getNodeState().testAndSet(NodeState.State.OUT_OF_DATE, NodeState.State.ACTIVE)) {
                installCheckpoint(myHandle);

                // Write collect from our new checkpoint to log and use that as the starting point for replay.
                //
                _storage.mark(new LiveWriter().write(myHandle.getLastCollect(), false), true);
            }

            /*
             * We do not want to allow a leader to immediately over-rule us, make it work a bit,
             * otherwise we risk leader jitter. This ensures we get proper leader leasing as per
             * live packet processing.
             */
            _common.leaderAction();
            signal(new StateEvent(StateEvent.Reason.UP_TO_DATE,
                    myHandle.getLastCollect().getMessage().getSeqNum(),
                    ((Collect) myHandle.getLastCollect().getMessage()).getRndNumber(),
                        Proposal.NO_VALUE, myHandle.getLastCollect().getSource()));

            _recoveryWindow.set(null);
            _cachedBegins.clear();

            return true;

        } finally {
            unguard();
        }
    }

    /* ********************************************************************************************
     *
     * Stats methods
     * 
     ******************************************************************************************** */

	long getHeartbeatCount() {
		return _receivedHeartbeats.longValue();
	}

	long getIgnoredCollectsCount() {
		return _ignoredCollects.longValue();
	}

    /* ********************************************************************************************
     *
     * Core message processing
     *
     ******************************************************************************************** */

    public boolean accepts(Transport.Packet aPacket) {
        EnumSet<PaxosMessage.Classification> myClassifications = aPacket.getMessage().getClassifications();

        return ((myClassifications.contains(PaxosMessage.Classification.ACCEPTOR_LEARNER))
                || (myClassifications.contains(PaxosMessage.Classification.RECOVERY)));
    }

    public void processMessage(Transport.Packet aPacket) {
        // Silently drop packets once we're shutdown - this is internal implementation so don't throw public exceptions
        //
        if (guard())
            return;

        try {
            // If we're not processing packets because we're out of date or because we're shutting down
            //
            if (_common.getNodeState().test(NodeState.State.OUT_OF_DATE)) {
                return;
            }

            PaxosMessage myMessage = aPacket.getMessage();
            long mySeqNum = myMessage.getSeqNum();

            if (_common.getNodeState().test(NodeState.State.RECOVERING)) {
                switch (myMessage.getType()) {
                    /*
                     * If the packet is a Need, we can process it for anything up to the last point of consistency
                     * in our log. Need's are never saved to the log so can only be received from other nodes looking
                     * to get themselves back up-to-date. This means there's no danger in us streaming loud and proud
                     * to that node using a LiveSender and a ReplayWriter.
                     */
                    case Operations.NEED : {
                        _logger.debug("Serving NEED from recovery " + aPacket);

                        process(aPacket, new ReplayWriter(0), new LiveSender());

                        return;
                    }

                    // If we're out of date, we need to get the user-code to find a checkpoint
                    //
                    case Operations.OUTOFDATE : {
                        synchronized(this) {
                            completedRecovery();
                            _common.getNodeState().set(NodeState.State.OUT_OF_DATE);
                        }

                        // Signal with node that pronounced us out of date - likely user code will get ckpt from there.
                        //
                        signal(new StateEvent(StateEvent.Reason.OUT_OF_DATE, mySeqNum,
                                _common.getLeaderRndNum(),
                                new Proposal(), aPacket.getSource()));
                        return;
                    }
                }
            }

            final Writer myWriter = new LiveWriter();
            int myProcessed;

            _sorter.add(aPacket);

            do {
                myProcessed =
                        _sorter.process(_common.getLowWatermark().getSeqNum(), new PacketSorter.PacketProcessor() {
                            public void consume(Transport.Packet aPacket) {
                                boolean myRecoveryInProgress = _common.getNodeState().test(NodeState.State.RECOVERING);
                                Sender mySender = ((myRecoveryInProgress) || (! _common.amMember())) ?
                                        new RecoverySender() : new LiveSender();

                                process(aPacket, myWriter, mySender);

                                if (myRecoveryInProgress) {
                                    if ((_common.getLowWatermark().getSeqNum() == _recoveryWindow.get().getMaxSeq()) &&
                                            (_common.getNodeState().testAndSet(NodeState.State.RECOVERING,
                                                    NodeState.State.ACTIVE))) {
                                        _common.resetLeaderAction();
                                        completedRecovery();
                                    }
                                }
                            }

                            public boolean recover(Need aNeed) {
                                boolean myResult = _common.getNodeState().testAndSet(NodeState.State.ACTIVE,
                                        NodeState.State.RECOVERING);

                                if (myResult) {
                                    _recoveryWindow.set(aNeed);

                                    /*
                                     * Both cachedBegins and acceptLedger run ahead of the low watermark thus if we're
                                     * entering recovery which starts at low watermark + 1, none of these are worth
                                     * keeping because we'll get them back as packets are streamed to us.
                                     */
                                    _cachedBegins.clear();
                                    _acceptLedgers.clear();

                                    /*
                                     * If we've just started up, neither the AL or Leader will have correct state.
                                     * Should our local leader be selected to lead it will almost certainly receive
                                     * OLD_ROUND from other nodes to bring it back into sync with sequence numbers.
                                     * However the local AL will accept the COLLECT which can then interfere with
                                     * recovery (which would be triggered by the local leader now it's in sync or
                                     * some other leader).
                                     *
                                     * To avoid such a situation, we dump a COLLECT that lies within the recovery window
                                     * which for this particular case would be COLLECT at seqnum = 0 with a window of
                                     * -1 to triggering packet seqnum - 1.
                                     */
                                    _logger.debug(AcceptorLearner.this.toString() + " Transition to recovery: " +
                                            Long.toHexString(_common.getLowWatermark().getSeqNum()));

                                    if (_common.getLastCollect().getMessage().getSeqNum() > aNeed.getMinSeq()) {
                                        _logger.warn(AcceptorLearner.this.toString() +
                                                " Current collect could interfere with recovery window - binning " +
                                            _common.getLastCollect().getMessage() + ", " + aNeed);

                                        _common.clearLeadership();
                                    }

                                    /*
                                     * Ask a node to bring us up to speed. Note that this node mightn't have all the
                                     * records we need.
                                     *
                                     * If that's the case, it won't stream at all or will only stream some part of our
                                     * recovery window. As the recovery watchdog measures progress through the recovery
                                     * window, a partial recovery or no recovery will be noticed and we'll ask a new
                                     * random node to bring us up to speed.
                                     */
                                    InetSocketAddress myNeedTarget = _common.getTransport().getFD().getRandomMember(
                                            _common.getTransport().getLocalAddress());

                                    if (myNeedTarget != null)
                                        new LiveSender().send(aNeed, myNeedTarget);

                                    // Startup recovery watchdog
                                    //
                                    reschedule();
                                }

                                return myResult;
                            }
                        });
            } while (myProcessed != 0);

        } finally {
            unguard();
        }
	}

    /* ********************************************************************************************
     *
     * Recovery logic - watchdogs, catchup and such
     *
     ******************************************************************************************** */

    private class Watchdog extends TimerTask {
        private final Watermark _past = _common.getLowWatermark();

        public void run() {
            if (! _common.getNodeState().test(NodeState.State.RECOVERING)) {
                // Someone else will do cleanup
                //
                return;
            }

            /*
             * If the watermark has advanced since we were started, recovery made some progress so we'll schedule a
             * future check otherwise fail.
             */
            if (! _past.equals(_common.getLowWatermark())) {
                _logger.debug(AcceptorLearner.this.toString() + " Recovery is progressing");

                reschedule();
            } else {
                _logger.warn(AcceptorLearner.this.toString() + " Recovery is NOT progressing - terminate");

                terminateRecovery();
                _common.getNodeState().testAndSet(NodeState.State.RECOVERING, NodeState.State.ACTIVE);
            }
        }
    }

    // Reschedule is only ever called when either we enter recovery (in which case there is no alarm) or
    // when the alarm has expired (thus does not need cancelling) when it is called from the alarm itself.
    //
    private void reschedule() {
        _logger.trace(toString() + " Rescheduling");

        TimerTask myAlarm = new Watchdog();

        if (_recoveryAlarm.compareAndSet(null, myAlarm)) {
            _common.getWatchdog().schedule(myAlarm, calculateRecoveryGracePeriod());
        }
    }

    void setRecoveryGracePeriod(long aPeriod) {
        if (guard())
            throw new IllegalStateException("Instance is shutdown");

        try {
            _gracePeriod.set(aPeriod);
        } finally {
            unguard();
        }
    }

    private long calculateRecoveryGracePeriod() {
        return _gracePeriod.get();
    }

    // Fold this away into recovery watchdog - terminate is only ever called from within the alarm itself
    // thus there's no need to cancel the alarm.
    //
    private void terminateRecovery() {
        _logger.debug(toString() + " Recovery terminate");

        /*
         * This will cause the AL to re-enter recovery when another packet is received and thus a new
         * NEED will be issued. Eventually we'll get too out-of-date or updated
         */
        _recoveryAlarm.set(null);
        _recoveryWindow.set(null);
    }

    private void completedRecovery() {
        _logger.debug(toString() + " Recovery complete");

        TimerTask myAlarm = _recoveryAlarm.getAndSet(null);
        if (myAlarm != null) {
            myAlarm.cancel();
            _common.getWatchdog().purge();
        }

        _recoveryWindow.set(null);
    }

    /* ********************************************************************************************
     *
     * Core state machine
     *
     ******************************************************************************************** */

	private void process(final Transport.Packet aPacket, Writer aWriter, Sender aSender) {
        PaxosMessage myMessage = aPacket.getMessage();
		InetSocketAddress myNodeId = aPacket.getSource();
		long mySeqNum = myMessage.getSeqNum();

		_logger.debug(toString() + " rxd " + myNodeId + " " + myMessage +
                ", loWmk " + Long.toHexString(_common.getLowWatermark().getSeqNum()));

		switch (myMessage.getType()) {
            case Operations.NEED : {
                final Need myNeed = (Need) myMessage;

                /*
                 * Make sure we can dispatch the recovery request - if the requested sequence number is less than
                 * our checkpoint low watermark we don't have the log available and thus we tell the AL it's out of
                 * date. Since Paxos will tend to keep replica's mostly in sync there's no need to see if other
                 * nodes pronounce out of date as likely they will, eventually (allowing for network instabilities).
                 */
                if (myNeed.getMinSeq() < _lastCheckpoint.get().getLowWatermark().getSeqNum()) {
                    _common.getTransport().send(_common.getTransport().getPickler().newPacket(new OutOfDate()),
                            aPacket.getSource());

                } else if (myNeed.getMaxSeq() <= _common.getLowWatermark().getSeqNum()) {
                    _logger.debug(toString() + " Running streamer");

                    new RemoteStreamer(aPacket.getSource(), myNeed).start();
                }

                break;
            }
			
			case Operations.COLLECT: {
				Collect myCollect = (Collect) myMessage;

				if (!_common.amAccepting(aPacket)) {
					_ignoredCollects.incrementAndGet();

					_logger.warn(toString() + " Not accepting: " + myCollect + ", "
							+ getIgnoredCollectsCount());
					return;
				}

				// If the collect supercedes our previous collect save it, return last proposal etc
				//
				if (_common.supercedes(aPacket)) {
                    _logger.trace(toString() + " Accepting collect: " + myCollect);

					aWriter.write(aPacket, true);
                    
                    aSender.send(constructLast(myCollect), myNodeId);

					/*
					 * If the collect comes from the current leader (has same rnd
					 * and node), we apply the multi-paxos optimisation, no need to
					 * save to disk, just respond with last proposal etc
					 */
				} else if (_common.sameLeader(aPacket)) {
                    aSender.send(constructLast(myCollect), myNodeId);

				} else {
                    _logger.warn(toString() + " Rejecting collect: " + myCollect + " against " +
                            _common.getLastCollect().getMessage());

					// Another collect has already arrived with a higher priority, tell the proposer it has competition
					//
                    aSender.send(new OldRound(_common.getLowWatermark().getSeqNum(),
                            _common.getLeaderAddress(), _common.getLeaderRndNum()), myNodeId);
				}
				
				break;
			}

			case Operations.BEGIN: {
				Begin myBegin = (Begin) myMessage;

				// If the begin matches the last round of a collect we're fine
				//
				if (_common.originates(aPacket)) {
					_common.leaderAction();

                    /*
                     * Special case, the leader could be playing catchup on the ledger because it never saw
                     * accepts (by virtue of packet loss or timeout) and has fallen back to a COLLECT which
                     * has produced LASTs which it's now trying to re-settle and move on. This is acceptable,
                     * just tell it ACCEPT again and let it move on.
                     */
                    if (mySeqNum <= _common.getLowWatermark().getSeqNum()) {
                        _logger.warn(toString() + " Leader is resync'ing with Ledger " + myBegin);

                        aSender.send(new Accept(mySeqNum, _common.getLeaderRndNum()),
                                _common.getTransport().getBroadcastAddress());

                    } else {
                        _cachedBegins.put(myBegin.getSeqNum(), myBegin);

                        aWriter.write(aPacket, true);

                        aSender.send(new Accept(mySeqNum, _common.getLeaderRndNum()),
                                _common.getTransport().getBroadcastAddress());

                        purgeAcceptLedger(myBegin);

                        Transport.Packet myLearned = tallyAccepts(myBegin);
                        if (myLearned != null)
                            learned(myLearned, aWriter);
                    }

				} else if (_common.precedes(aPacket)) {
					// New collect was received since the collect for this begin,
					// tell the proposer it's got competition
					//
					aSender.send(new OldRound(_common.getLowWatermark().getSeqNum(),
                            _common.getLeaderAddress(), _common.getLeaderRndNum()), myNodeId);
				} else {
					/*
					 * Quiet, didn't see the collect, leader hasn't accounted for
					 * our values, it hasn't seen our last and we're likely out of sync with the majority
					 */
					_logger.warn(toString() + " Missed collect, going silent: " + mySeqNum
							+ " [ " + myBegin.getRndNumber() + " ], ");
				}
				
				break;
			}

            case Operations.ACCEPT: {
                Accept myAccept = (Accept) myMessage;

                // Don't process a value we've already learnt...
                //
                if (myAccept.getSeqNum() <= _common.getLowWatermark().getSeqNum())
                    return;

                getAndCreateAcceptLedger(aPacket).add(aPacket);

                Begin myCachedBegin = _cachedBegins.get(myAccept.getSeqNum());

                if (myCachedBegin != null) {
                    Transport.Packet myLearned = tallyAccepts(myCachedBegin);
                    if (myLearned != null)
                        learned(myLearned, aWriter);
                }

                break;
            }

            /*
             * LEARNED will only be processed on a recovery run. It is generated and logged as the result of tallying
             * ACCEPTs from all AL's in the cluster and BEGINs from Leaders. It is NEVER, sent across the network
             * by a leader but MAY be sent from another AL in response to a NEED. This latter exposes a risk of
             * packet loss causing the prior BEGIN to go missing which we must allow for.
             *
             * ACCEPTs themselves are never logged individually or even as a group.
             */
			case Operations.LEARNED: {
                if (_cachedBegins.get(myMessage.getSeqNum()) != null)
                    learned(aPacket, aWriter);

				break;
			}

			default:
				throw new RuntimeException("Unexpected message" + ", " + _common.getTransport().getLocalAddress());
		}
	}

    /**
     * Perform all actions associated with the act of learning a value for a sequence number.
     *
     * @param aPacket
     * @param aWriter
     */
    private void learned(Transport.Packet aPacket, Writer aWriter) {
        Learned myLearned = (Learned) aPacket.getMessage();
        long mySeqNum = myLearned.getSeqNum();

        _common.leaderAction();

        _acceptLedgers.remove(myLearned.getSeqNum());

        Begin myBegin = _cachedBegins.remove(mySeqNum);

        // Record the learned value even if it's the heartbeat so there are no gaps in the Paxos sequence
        //
        long myLogOffset = aWriter.write(aPacket, true);

        _common.install(new Watermark(mySeqNum, myLogOffset));

        if (myBegin.getConsolidatedValue().equals(LeaderFactory.HEARTBEAT)) {
            _receivedHeartbeats.incrementAndGet();

            _logger.trace(toString() + " discarded heartbeat: "
                    + System.currentTimeMillis() + ", "
                    + getHeartbeatCount());
        } else {
            _logger.debug(toString() + " Learnt value: " + mySeqNum);

            signal(new StateEvent(StateEvent.Reason.VALUE, mySeqNum,
                    _common.getLeaderRndNum(),
                    myBegin.getConsolidatedValue(), aPacket.getSource()));
        }
    }

    /**
     * Utility method to manage the lifecycle of creating an accept ledger.
     *
     * @param anAccept
     * @return the newly or previously created ledger for the specified sequence number.
     */
    private AcceptLedger getAndCreateAcceptLedger(Transport.Packet anAccept) {
        Long mySeqNum = anAccept.getMessage().getSeqNum();
        AcceptLedger myAccepts = _acceptLedgers.get(mySeqNum);

        if (myAccepts == null) {
            AcceptLedger myInitial = new AcceptLedger(mySeqNum);
            AcceptLedger myResult = _acceptLedgers.put(mySeqNum, myInitial);

            myAccepts = ((myResult == null) ? myInitial : myResult);
        }

        return myAccepts;
    }

    /**
     * Remove any accepts in the ledger not appropriate for the passed begin. We must tally only those accepts
     * that match the round and sequence number of this begin. All others should be flushed.
     *
     * @param aBegin
     */
    private void purgeAcceptLedger(Begin aBegin) {
        AcceptLedger myAccepts = _acceptLedgers.get(aBegin.getSeqNum());

        if (myAccepts != null)
            myAccepts.purge(aBegin);
    }

    /**
     * Determines whether a sufficient number of accepts can be tallied against the specified begin.
     *
     * @param aBegin
     * @return A Learned packet to be logged if there are sufficient accepts, <code>null</code> otherwise.
     */
    private Transport.Packet tallyAccepts(Begin aBegin) {
        AcceptLedger myAccepts = _acceptLedgers.get(aBegin.getSeqNum());

        if (myAccepts != null) {
            Learned myLearned = myAccepts.tally(aBegin, _common.getTransport().getFD().getMajority());

            if (myLearned != null) {
                _logger.trace(toString() + " *** Speculative COMMIT possible ***");

                return _common.getTransport().getPickler().newPacket(myLearned);
            }
        }

        return null;
    }

	private PaxosMessage constructLast(Collect aCollect) {
        long mySeqNum = aCollect.getSeqNum();
		Watermark myLow = _common.getLowWatermark();

		Begin myState;
		
		try {
            // If we know nothing, we must start from beginning of log otherwise we start from the low watermark.
            //
			if (myLow.equals(Watermark.INITIAL)) {
				myState = new StateFinder(mySeqNum, 0).getState();
			} else 
				myState = new StateFinder(mySeqNum, myLow.getLogOffset()).getState();
		} catch (Exception anE) {
			_logger.error(toString() + "Failed to replay log", anE);
			throw new RuntimeException(toString() + "Failed to replay log", anE);
		}
		
		if (myState != null) {
            return new Last(mySeqNum, myLow.getSeqNum(), myState.getRndNumber(), myState.getConsolidatedValue());
		} else {
            /*
             * No state found. If we've gc'd and checkpointed, we can't provide an answer. In such a case,
             * the leader is out of date and we tell them. Otherwise, we're clean and give the leader a green light.
             */
            if (mySeqNum <= myLow.getSeqNum())
                return new OldRound(myLow.getSeqNum(), _common.getLeaderAddress(),
                        _common.getLeaderRndNum());
            else
                return new Last(mySeqNum, myLow.getSeqNum(),
                        Long.MIN_VALUE, Proposal.NO_VALUE);
        }
	}

    interface Sender {
        public void send(PaxosMessage aMessage, InetSocketAddress aNodeId);
    }
    
    class LiveSender implements Sender {
        public void send(PaxosMessage aMessage, InetSocketAddress aNodeId) {
            // Go silent if we're shutting down - shutdown process can be brutal, we don't want to send out lies
            //
            if (_common.getNodeState().test(NodeState.State.SHUTDOWN))
                return;

            _logger.debug(AcceptorLearner.this.toString() + " sending " + aMessage + " to " + aNodeId);
            _common.getTransport().send(_common.getTransport().getPickler().newPacket(aMessage), aNodeId);
        }        
    }
    
    class RecoverySender implements Sender {
        public void send(PaxosMessage aMessage, InetSocketAddress aNodeId) {
            // Drop silently
        }
    }
    
    /* ********************************************************************************************
     *
     * Operation log handling
     *
     ******************************************************************************************** */

    interface Writer {
        public long write(Transport.Packet aPacket, boolean aForceRequired);
    }
    
    class ReplayWriter implements Writer {
        private final long _offset;

        ReplayWriter(long anOffset) {
            _offset = anOffset;
        }

        public long write(Transport.Packet aPacket, boolean aForceRequired) {
            return _offset;
        }
    }

    class LiveWriter implements Writer {
        public long write(Transport.Packet aPacket, boolean aForceRequired) {
            try {
                return _storage.put(_common.getTransport().getPickler().pickle(aPacket), aForceRequired);
            } catch (Exception anE) {
                _logger.error(AcceptorLearner.this.toString() + " cannot log: " + System.currentTimeMillis(), anE);
                throw new RuntimeException(anE);
            }            
        }
    }
    
    /**
     * Used to locate the recorded state of a specified instance of Paxos.
     */
    private class StateFinder implements Consumer {
        private Begin _lastBegin;

        StateFinder(long aSeqNum, long aLogOffset) throws Exception {
            new LogRangeProducer(aSeqNum - 1, aSeqNum, this, _storage,
                    _common.getTransport().getPickler()).produce(aLogOffset);
        }

        Begin getState() {
            return _lastBegin;
        }

        public void process(Transport.Packet aPacket, long aLogOffset) {
            PaxosMessage myMessage = aPacket.getMessage();

            if (myMessage.getType() == Operations.BEGIN) {
                Begin myBegin = (Begin) myMessage;

                if (_lastBegin == null) {
                    _lastBegin = myBegin;
                } else if (myBegin.getRndNumber() > _lastBegin.getRndNumber()) {
                    _lastBegin = myBegin;
                }
            }
        }
    }


    /**
     * Recovers all state since a particular instance of Paxos up to and including a specified maximum instance
     * and dispatches them to a particular remote node.
     */
    private class RemoteStreamer extends Thread implements Consumer {
        private final Need _need;
        private final InetSocketAddress _target;

        RemoteStreamer(InetSocketAddress aTarget, Need aNeed) {
            _need = aNeed;
            _target = aTarget;
        }

        public void run() {
            if (guard()) {
                _logger.warn(AcceptorLearner.this.toString() + " Aborting RemoteStreamer");
                return;
            } else {
                _logger.debug(AcceptorLearner.this.toString() + " RemoteStreamer starting");
            }

            try {
                new LogRangeProducer(_need.getMinSeq(), _need.getMaxSeq(), this, _storage,
                        _common.getTransport().getPickler()).produce(0);
            } catch (Exception anE) {
                _logger.error(AcceptorLearner.this.toString() + " Failed to replay log", anE);
            } finally {
                unguard();
            }
        }

        public void process(Transport.Packet aPacket, long aLogOffset) {
            _logger.trace(AcceptorLearner.this.toString() + " Streaming: " + aPacket);
            _common.getTransport().send(aPacket, _target);
        }
    }

    void add(Listener aListener) {
        synchronized(_listeners) {
            _listeners.add(aListener);
        }
    }

    private void signal(StateEvent aStatus) {
        for (Listener myTarget : _listeners)
            myTarget.transition(aStatus);
    }

    public String toString() {
        return "AL [ " + _common.getTransport().getLocalAddress() + " ]";
    }
}
