package org.dancres.paxos.impl;

import org.dancres.paxos.*;
import org.dancres.paxos.bus.Messages;
import org.dancres.paxos.messages.*;
import org.dancres.paxos.messages.codec.Codecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

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
public class AcceptorLearner implements Paxos.CheckpointFactory, MessageProcessor, Messages.Subscriber<Constants.EVENTS> {
    static final String HEARTBEAT_KEY = "org.dancres.paxos.Hbt";
    static final String MEMBER_CHANGE_KEY = "org.dancres.paxos.MemChg";

    private static final long DEFAULT_RECOVERY_GRACE_PERIOD = 5 * 1000;

	private static final Logger _logger = LoggerFactory.getLogger(AcceptorLearner.class);

    private final StatsImpl _stats = new StatsImpl();
    private final AtomicLong _gracePeriod = new AtomicLong(DEFAULT_RECOVERY_GRACE_PERIOD);

    private final AtomicReference<TimerTask> _recoveryAlarm = new AtomicReference<>(null);
    private final AtomicReference<Need> _recoveryWindow = new AtomicReference<>(null);

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
    private final AcceptLedger _acceptLedgers;

    private final Lock _guardLock = new ReentrantLock();
    private final Condition _notActive = _guardLock.newCondition();
    private int _activeCount;

    private final Consensus.StateMachine _stateMachine;

    private final AtomicLong _rockBottom = new AtomicLong(Watermark.INITIAL.getSeqNum());
    private final AtomicReference<Watermark> _lowWatermark =
            new AtomicReference<>(Watermark.INITIAL);

    private final Messages.Subscription<Constants.EVENTS> _bus;

    /* ********************************************************************************************
     *
     * Operation log handling
     *
     ******************************************************************************************** */

    private interface Writer extends BiFunction<Transport.Packet, Boolean, Long> {
    }

    private final Writer _liveWriter;

    private interface Sender extends BiConsumer<PaxosMessage, InetSocketAddress> {
    }

    private final Sender _recoverySender = (aMessage, anAddress) -> {};
    private final Sender _liveSender;

    /* ********************************************************************************************
     *
     * Lifecycle, checkpointing and open/close
     *
     ******************************************************************************************** */

    AcceptorLearner(LogStorage aStore, Common aCommon) {
        _storage = aStore;
        _common = aCommon;
        _bus = aCommon.getBus().subscribe("AcceptorLearner", this);

        _liveWriter = (aPacket, aForce) -> {
            try {
                return _storage.put(_common.getTransport().getPickler().pickle(aPacket), aForce);
            } catch (Exception anE) {
                _logger.error(AcceptorLearner.this.toString() + " cannot log: " + System.currentTimeMillis(), anE);
                throw new RuntimeException(anE);
            }
        };

        _liveSender = (aMessage, anAddress) -> {
            // Go silent if we're shutting down - shutdown process can be brutal, we don't want to send out lies
            //
            if (_common.getNodeState().test(NodeState.State.SHUTDOWN))
                return;

            if (aMessage.getType() == PaxosMessage.Types.ACCEPT)
                _stats.incrementAccepts();

            _logger.debug(AcceptorLearner.this.toString() + " sending " + aMessage + " to " + anAddress);
            _common.getTransport().send(_common.getTransport().getPickler().newPacket(aMessage), anAddress);
        };

        _acceptLedgers = new AcceptLedger(toString());
        _stateMachine = new Consensus.StateMachine(_common.getTransport().getLocalAddress().toString(), _stats);
    }

    interface CheckpointConsumer extends Function<CheckpointHandle, Boolean> {
    }

    @Override
    public Paxos.Checkpoint forSaving() {
        if (_common.getNodeState().test(NodeState.State.OUT_OF_DATE))
            throw new IllegalStateException("Instance is out of date");
        
        return new Paxos.Checkpoint() {
            private final CheckpointHandle _handle = newCheckpoint();

            @Override
            public CheckpointHandle getHandle() {
                return _handle;
            }

            @Override
            public CheckpointConsumer getConsumer() {
                return (ch) -> {
                    try {
                        if (! (ch instanceof CheckpointHandleImpl))
                            throw new IllegalArgumentException("Where did you get this handle?");

                        return saved((CheckpointHandleImpl) ch);
                    } catch (Exception anE) {
                        _logger.error("Serious storage problem: ", anE);
                        
                        throw new Error("Serious storage problem");
                    }
                };
            }
        };
    }

    @Override
    public Paxos.Checkpoint forRecovery() {
        return new Paxos.Checkpoint() {
            private final CheckpointHandle _handle = newCheckpoint();

            @Override
            public CheckpointHandle getHandle() {
                return _handle;
            }

            @Override
            public CheckpointConsumer getConsumer() {
                return (ch) -> {
                    if ((ch == null) || (ch == _handle) || (ch.equals(CheckpointHandle.NO_CHECKPOINT)) ||
                            (! (ch instanceof CheckpointHandleImpl)))
                        throw new IllegalArgumentException(
                                "Not an acceptable - can't be null, NO_CHECKPOINT or the handled provided from forRecovery");

                    try {
                        return bringUpToDate((CheckpointHandleImpl) ch);
                    } catch (Exception anE) {
                        _logger.error("Serious storage problem: ", anE);

                        throw new Error("Serious storage problem");
                    }
                };
            }
        };
    }

    @Override
    public void msg(Messages.Message<Constants.EVENTS> aMessage) {
    }

    private void signal(StateEvent aStatus) {
        _bus.send(Constants.EVENTS.AL_TRANSITION, aStatus);
    }

    private void guardWithException() {
        if (guard())
            throw new IllegalStateException("Instance is shutdown");
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

    static class LedgerPosition {
        private final long _seqNum;
        private final long _rndNum;

        private LedgerPosition(long aSeqNum, long aRndNum) {
            _seqNum = aSeqNum;
            _rndNum = aRndNum;
        }

        long getSeqNum() {
            return _seqNum;
        }

        long getRndNum() {
            return _rndNum;
        }
    }

    /**
     * If this method fails be sure to invoke close regardless.
     *
     * @param aHandle
     * @throws Exception
     */
    public LedgerPosition open(CheckpointHandle aHandle) throws Exception {
        _storage.open();

        _common.getNodeState().set(NodeState.State.RECOVERING);

        long myStartSeqNum = Watermark.INITIAL.getSeqNum();

        if (!aHandle.equals(CheckpointHandle.NO_CHECKPOINT)) {
            if (aHandle instanceof CheckpointHandleImpl) {
                CheckpointHandleImpl myHandle = (CheckpointHandleImpl) aHandle;
                testAndSetLast(myHandle);
                myStartSeqNum = install(myHandle);
            } else
                throw new IllegalArgumentException("Not a valid CheckpointHandle: " + aHandle);
        }

        try {
            new LogRangeProducer(myStartSeqNum, Long.MAX_VALUE, (Transport.Packet aPacket, long aLogOffset) ->
                    AcceptorLearner.this.process(aPacket,
                            (aReplayPacket, aSender) -> aLogOffset,
                            _recoverySender),
                    _storage, _common.getTransport().getPickler()).produce(0);
        } catch (Exception anE) {
            _logger.error(toString() + " Failed to replay log", anE);
        }

        if (!_common.getNodeState().testAndSet(NodeState.State.RECOVERING, NodeState.State.ACTIVE))
            throw new Error("Serious state issue at open");

        return new LedgerPosition(_lowWatermark.get().getSeqNum(), _stateMachine.getElected().getRndNumber());
    }

    public void close() {
        try {
            /*
             * Allow for the fact we might be actively processing packets. Mark ourselves shutdown and drain...
             */
            if (! _common.getNodeState().test(NodeState.State.ACTIVE)) {
                _logger.warn(this + " not active at close: " + _common.getNodeState().toString());
            }

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

            _stateMachine.resetElected();
            _lowWatermark.set(Watermark.INITIAL);

            _sorter.clear();
            _cachedBegins.clear();

            _storage.close();

        } catch (Exception anE) {
            _logger.error(toString() + " Failed to close storage", anE);
            throw new Error(anE);
        }
    }

    CheckpointHandle newCheckpoint() {
        guardWithException();

        try {
            // Low watermark will always lag collect so no need for atomicity here
            //
            return new CheckpointHandleImpl(_lowWatermark.get(),
                    _common.getTransport().getPickler().newPacket(_stateMachine.getElected(),
                            _stateMachine.getElector()),
                    _common.getTransport().getPickler());
        } finally {
            unguard();
        }
    }

    private boolean saved(CheckpointHandleImpl aHandle) throws Exception {
        guardWithException();

        try {
            if (testAndSetLast(aHandle)) {
                _storage.mark(aHandle.getLowWatermark().getLogOffset(), true);
                return true;
            } else
                return false;
        } finally {
            unguard();
        }
    }

    private boolean testAndSetLast(CheckpointHandleImpl aHandle) {
        long myProposedNeedLimit = aHandle.getLowWatermark().getSeqNum();

        // If the checkpoint we're installing is newer...
        //
        long myCurrentNeedLimit;
        do {
            myCurrentNeedLimit = _rockBottom.get();

            if (myProposedNeedLimit <= myCurrentNeedLimit) {
                _logger.info(this + " proposed need limit is not new enough: " + myCurrentNeedLimit +
                        " vs " + myProposedNeedLimit);

                return false;
            }

        } while (! _rockBottom.compareAndSet(myCurrentNeedLimit, myProposedNeedLimit));

        return true;
    }

    private long install(CheckpointHandleImpl aHandle) {
        _stateMachine.forciblyAnoint((Collect) aHandle.getLastCollect().getMessage(),
                aHandle.getLastCollect().getSource());

        _logger.info(toString() + " Checkpoint installed: " + aHandle.getLastCollect() + " @ " +
                aHandle.getLowWatermark());

        _lowWatermark.set(aHandle.getLowWatermark());
        _sorter.recoveredToCheckpoint(aHandle.getLowWatermark().getSeqNum());
        
        return aHandle.getLowWatermark().getSeqNum();        
    }

    /**
     * When an AL is out of date, call this method to bring it back into sync from a remotely sourced
     * checkpoint.
     *
     * @param aHandle obtained from the remote checkpoint.
     * @throws Exception
     */
    private boolean bringUpToDate(CheckpointHandleImpl aHandle) throws Exception {
        guardWithException();

        try {
            if (! _common.getNodeState().test(NodeState.State.OUT_OF_DATE))
                throw new IllegalStateException("Not out of date");

            if (! testAndSetLast(aHandle))
                return false;

            install(aHandle);

            // Write collect from our new checkpoint to log and use that as the starting point for replay.
            //
            _storage.mark(_liveWriter.apply(aHandle.getLastCollect(), false), true);

            /*
             * If we're out of date, there will be no active timers and no activity on the log.
             * We should install the checkpoint and clear out all state associated with known instances
             * of Paxos. Additional state will be obtained from another AL via the lightweight recovery protocol.
             * Out of date means that the existing log has no useful data within it implying it can be discarded.
             */
            if (! _common.getNodeState().testAndSet(NodeState.State.OUT_OF_DATE, NodeState.State.ACTIVE))
                throw new Error("Serious recovery state issue - this shouldn't happen");


            _logger.info(this + " restored to active");

            /*
             * We do not want to allow a leader to immediately over-rule current, make it work a bit,
             * otherwise we risk leader jitter. This ensures we get proper leader leasing as per
             * live packet processing.
             */
            _stateMachine.extendExpiry();
            signal(new StateEvent(StateEvent.Reason.UP_TO_DATE,
                    aHandle.getLastCollect().getMessage().getSeqNum(),
                    ((Collect) aHandle.getLastCollect().getMessage()).getRndNumber(),
                    Proposal.NO_VALUE));

            _recoveryWindow.set(null);
            _cachedBegins.clear();

            return true;

        } finally {
            unguard();
        }
    }

    Stats getStats() {
        return _stats;
    }

    Watermark getLowWatermark() {
        return _lowWatermark.get();
    }

    public long getLastSeq() { return _lowWatermark.get().getSeqNum(); }

    long getLeaderRndNum() {
        return _stateMachine.getElected().getRndNumber();
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
            PaxosMessage myMessage = aPacket.getMessage();
            int myProcessed;

            _logger.debug(toString() + " queuing " + aPacket.getSource() + ", " + myMessage +
                    ", loWmk " + Long.toHexString(_lowWatermark.get().getSeqNum()));

            _sorter.add(aPacket);

            /*
              PacketSorter only feeds us packets at <= low_watermark + 1.

              Need and OutOfDate will thus always be processed owing to being stamped with seq = Constants.RECOVERY_SEQ.

              Anything at low_watermark will be old_rounded etc. Anything at + 1 will be processed to make progress.

              When we're pronounced OUT_OF_DATE PacketSorter will implicitly ensure we don't process newer packets but
              we must inhibit the other cases hence the check for OUT_OF_DATE below and the fiddling with mySender.

              The last piece of the puzzle is NEED messages which we can always serve regardless of state. In that
              specific case, we ignore which sender is specified using _liveSender regardless - see RemoteStreamer.
              */
            do {
                myProcessed =
                        _sorter.process(_lowWatermark.get(), new PacketSorter.PacketProcessor() {
                            public void consume(Transport.Packet aPacket) {
                                boolean myRecoveryInProgress =
                                        _common.getNodeState().test(NodeState.State.RECOVERING);
                                
                                Sender mySender = (_common.getNodeState().test(NodeState.State.OUT_OF_DATE) ||
                                        myRecoveryInProgress || (! _common.amMember())) ?
                                        _recoverySender : _liveSender;

                                process(aPacket, _liveWriter, mySender);

                                if (myRecoveryInProgress) {
                                    if ((_lowWatermark.get().getSeqNum() == _recoveryWindow.get().getMaxSeq()) &&
                                            (_common.getNodeState().testAndSet(NodeState.State.RECOVERING,
                                                    NodeState.State.ACTIVE))) {
                                        _stateMachine.resetExiry();
                                        completedRecovery();
                                    }
                                }
                            }

                            public boolean recover(Need aNeed, InetSocketAddress aSourceAddr) {
                                boolean myResult = _common.getNodeState().testAndSet(NodeState.State.ACTIVE,
                                        NodeState.State.RECOVERING);

                                if (myResult) {
                                    _recoveryWindow.set(aNeed);
                                    _stats.incrementRecoveries();

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
                                            Long.toHexString(_lowWatermark.get().getSeqNum()));

                                    if (_stateMachine.getElected().getSeqNum() > aNeed.getMinSeq()) {
                                        _logger.warn(AcceptorLearner.this.toString() +
                                                " Current collect could interfere with recovery window - binning " +
                                            _stateMachine.getElected() + ", " + aNeed);

                                        _stateMachine.resetElected();
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

                                    /*
                                     * Prefer random selection as it helps spread load but fallback to source node
                                     * (likely the current leader) if all else fails (e.g. because we have no valid
                                     * membership).
                                     */
                                    if (myNeedTarget != null)
                                        _liveSender.accept(aNeed, myNeedTarget);
                                    else
                                        _liveSender.accept(aNeed, aSourceAddr);

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
        private final Watermark _past = _lowWatermark.get();

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
            if (! _past.equals(_lowWatermark.get())) {
                _logger.debug(AcceptorLearner.this.toString() + " Recovery is progressing");

                _recoveryAlarm.set(null);
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
        guardWithException();

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
        _logger.debug(toString() + " Recovery complete: " + _common.getNodeState());

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

		switch (myMessage.getType()) {
            // If we're out of date, we need to get the user-code to find a checkpoint
            //
            case PaxosMessage.Types.OUTOFDATE : {
                // If we're not already reported out-of-date...
                //
                if (_common.getNodeState().testAndSet(NodeState.State.RECOVERING, NodeState.State.OUT_OF_DATE)) {

                    completedRecovery();
                    _logger.warn(toString() + " Moved to OUT_OF_DATE");

                    // @todo Signal with node that pronounced us out of date - likely user code will get ckpt from there.
                    //
                    signal(new StateEvent(StateEvent.Reason.OUT_OF_DATE, mySeqNum,
                            _stateMachine.getElected().getRndNumber(),
                            Proposal.NO_VALUE));
                }

                break;
            }

            // We can always process these with a live sender, RemoteStreamer thus does not use aSender
            //
            case PaxosMessage.Types.NEED : {
                final Need myNeed = (Need) myMessage;

                /*
                 * Make sure we can dispatch the recovery request - if the requested sequence number is less than
                 * our checkpoint low watermark we don't have the log available and thus we tell the AL it's out of
                 * date. Since Paxos will tend to keep replica's mostly in sync there's no need to see if other
                 * nodes pronounce out of date as likely they will, eventually (allowing for network instabilities).
                 */
                if (myNeed.getMinSeq() < _rockBottom.get()) {
                    _logger.warn("Need is too old: " + myNeed + " from: " + myNodeId);

                    _common.getTransport().send(_common.getTransport().getPickler().newPacket(new OutOfDate()),
                            aPacket.getSource());
                } else {
                    _logger.debug(toString() + " Running streamer -> " + myNodeId + " partial coverage: " +
                        (myNeed.getMaxSeq() > _lowWatermark.get().getSeqNum()));

                    new RemoteStreamer(aPacket.getSource(), myNeed).start();
                }

                break;
            }
			
			case PaxosMessage.Types.COLLECT: {
				Collect myCollect = (Collect) myMessage;

				_stateMachine.dispatch(myCollect, aPacket.getSource(), _lowWatermark.get(),
                        (anElectedRnd, anElector) -> {
                            // Another collect has already arrived with a higher priority, tell the proposer it has competition
                            //
                            aSender.accept(new OldRound(_lowWatermark.get().getSeqNum(),
                                    anElector, anElectedRnd), myNodeId);
                        },
                        (claim, mustWrite) -> {
                            if (mustWrite)
                                aWriter.apply(aPacket, true);

                            aSender.accept(constructLast(myCollect), myNodeId);

                            signal(new StateEvent(StateEvent.Reason.NEW_LEADER, mySeqNum,
                                    claim.getRndNumber(),
                                    Proposal.NO_VALUE));

                        },
                        _common.getNodeState().test(NodeState.State.RECOVERING));

				break;
			}

			case PaxosMessage.Types.BEGIN: {
				Begin myBegin = (Begin) myMessage;

				_stateMachine.dispatch(myBegin, _lowWatermark.get(),
                        (anElectedRnd, anElector) -> aSender.accept(new OldRound(_lowWatermark.get().getSeqNum(),
                                anElector, anElectedRnd), myNodeId),
                        (claim, mustWrite) -> {
                            if (mustWrite) {
                                _cachedBegins.put(myBegin.getSeqNum(), myBegin);

                                aWriter.apply(aPacket, true);

                                aSender.accept(new Accept(mySeqNum, _stateMachine.getElected().getRndNumber()),
                                        _common.getTransport().getBroadcastAddress());

                                _acceptLedgers.purgeAcceptLedger(myBegin);

                                Learned myLearned = _acceptLedgers.tallyAccepts(myBegin,
                                        _common.getTransport().getFD().getMajority());
                                if (myLearned != null)
                                    learned(_common.getTransport().getPickler().newPacket(myLearned), aWriter);

                            } else {
                                /*
                                 * Special case, the leader could be playing catchup on the ledger because it never saw
                                 * accepts (by virtue of packet loss or timeout) and has fallen back to a COLLECT which
                                 * has produced LASTs which it's now trying to re-settle and move on. This is acceptable,
                                 * just tell it ACCEPT again and let it move on.
                                 */
                                aSender.accept(new Accept(mySeqNum, _stateMachine.getElected().getRndNumber()),
                                        _common.getTransport().getBroadcastAddress());
                            }
                        });

				break;
			}

            case PaxosMessage.Types.ACCEPT: {
                Accept myAccept = (Accept) myMessage;

                // Don't process a value we've already learnt...
                //
                if (myAccept.getSeqNum() <= _lowWatermark.get().getSeqNum())
                    return;

                _acceptLedgers.extendLedger(aPacket);

                Begin myCachedBegin = _cachedBegins.get(myAccept.getSeqNum());

                if (myCachedBegin != null) {
                    Learned myLearned = _acceptLedgers.tallyAccepts(myCachedBegin,
                            _common.getTransport().getFD().getMajority());
                    if (myLearned != null)
                        learned(_common.getTransport().getPickler().newPacket(myLearned), aWriter);
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
			case PaxosMessage.Types.LEARNED: {
                if (_cachedBegins.get(myMessage.getSeqNum()) != null)
                    learned(aPacket, aWriter);

				break;
			}

			default:
                throw new Error("Unexpected message of type " + myMessage.getType() + ", " +
                        _common.getTransport().getLocalAddress());		}
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

        _stateMachine.extendExpiry();

        _acceptLedgers.remove(myLearned.getSeqNum());

        Begin myBegin = _cachedBegins.remove(mySeqNum);

        // Record the learned value even if it's the heartbeat so there are no gaps in the Paxos sequence
        //
        long myLogOffset = aWriter.apply(aPacket, true);

        _lowWatermark.set(new Watermark(mySeqNum, myLogOffset));

        if (myBegin.getConsolidatedValue().get(HEARTBEAT_KEY) != null) {
            _stats.incrementHeartbeats();

            _logger.trace(toString() + " discarded heartbeat: "
                    + System.currentTimeMillis() + ", "
                    + _stats.getHeartbeatCount());
        } else if (myBegin.getConsolidatedValue().get(MEMBER_CHANGE_KEY) != null) {
            _logger.trace(toString() + " membership change received");

            Collection<InetSocketAddress> myAddrs =
                    Codecs.expand(myBegin.getConsolidatedValue().get(MEMBER_CHANGE_KEY));

            _logger.debug(toString() + " membership changed to " + myAddrs);

            _common.getTransport().getFD().pin(myAddrs);
        } else {
            _logger.debug(toString() + " Learnt value: " + mySeqNum);

            signal(new StateEvent(StateEvent.Reason.VALUE, mySeqNum,
                    _stateMachine.getElected().getRndNumber(),
                    myBegin.getConsolidatedValue()));
        }
    }

	private PaxosMessage constructLast(Collect aCollect) {
        long mySeqNum = aCollect.getSeqNum();
		Watermark myLow = _lowWatermark.get();

		Begin myState;
		
		try {
            // If we know nothing, we must start from beginning of log otherwise we start from the low watermark.
            //
			if (myLow.equals(Watermark.INITIAL)) {
				myState = new StateFinder(mySeqNum, 0).getState();
			} else 
				myState = new StateFinder(mySeqNum, myLow.getLogOffset()).getState();
		} catch (Exception anE) {
			_logger.error(toString() + " Failed to replay log", anE);
			throw new RuntimeException(toString() + "Failed to replay log", anE);
		}
		
		if (myState != null) {
            return new Last(mySeqNum, myLow.getSeqNum(), myState.getRndNumber(), myState.getConsolidatedValue());
		} else {
            /*
             * No state found. If we've gc'd and checkpointed, we can't provide an answer. In such a case,
             * the leader is out of date and we tell them. Otherwise, we're clean and give the leader a green light.
             */
            if (mySeqNum <= myLow.getSeqNum()) {
                _logger.warn(toString() + " OLDROUND - LDOUT " + mySeqNum + " vs " + myLow.getSeqNum());

                return new OldRound(myLow.getSeqNum(),_stateMachine.getElector(),
                        _stateMachine.getElected().getRndNumber());
            } else
                return new Last(mySeqNum, myLow.getSeqNum(),
                        Constants.PRIMORDIAL_RND, Proposal.NO_VALUE);
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

            if (myMessage.getType() == PaxosMessage.Types.BEGIN) {
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

    public String toString() {
        return "AL [ " + _common.getTransport().getLocalAddress() + " ]";
    }
}
