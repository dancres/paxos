package org.dancres.paxos.impl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.dancres.paxos.CheckpointHandle;
import org.dancres.paxos.LogStorage;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.messages.*;
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
 * @todo Arrange for checkpoint handle to contain a packet pickler reference.
 * That pickler can be used to flatten a packet containing collect etc into
 * a byte[]. We then serialize the Pickler and save the byte[].
 * We then reverse this process for de-serialization - deserialize the pickler, 
 * recover the byte[] and use the pickler to unpack it.
 *
 * The initial value of _lastCheckpoint can be setup with an initial checkpoint
 * that includes a reference to the transport's current pickler. BUT BEWARE:
 * A number of the existing tests currently return null in response to a call
 * to getPickler() so will need fixing to return some meaningful pickler
 * implementation.
 *
 * @author dan
 */
public class AcceptorLearner {
    private static final long DEFAULT_RECOVERY_GRACE_PERIOD = 30 * 1000;

    // Pause should be less than DEFAULT_GRACE_PERIOD
    //
    private static final long SHUTDOWN_PAUSE = 1 * 1000;

	private static final Logger _logger = LoggerFactory.getLogger(AcceptorLearner.class);

	/**
	 * Statistic that tracks the number of Collects this AcceptorLearner ignored
	 * from competing leaders within DEFAULT_LEASE ms of activity from the
	 * current leader.
	 */
	private final AtomicLong _ignoredCollects = new AtomicLong();
	private final AtomicLong _receivedHeartbeats = new AtomicLong();

    private long _gracePeriod = DEFAULT_RECOVERY_GRACE_PERIOD;

    private TimerTask _recoveryAlarm = null;
    private Need _recoveryWindow = null;

	private final BlockingQueue<Transport.Packet> _packetBuffer = new LinkedBlockingQueue<Transport.Packet>();

	private final LogStorage _storage;
    private final Common _common;
	private final InetSocketAddress _localAddress;

    /**
     * Begins contain the values being proposed. These values must be remembered from round to round of an instance
     * until it has been committed. For any instance we believe is unresolved we keep the last value proposed (from
     * the last acceptable Begin) cached (the begins are also logged to disk) such that when we see Success for an
     * instance we remove the value from the cache and send it to any listeners. This saves us having to disk scans
     * for values and also placing the value in the Success message.
     */
    private final Map<Long, Begin> _cachedBegins = new ConcurrentHashMap<Long, Begin>();

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
        private transient AtomicReference<AcceptorLearner> _al = new AtomicReference<AcceptorLearner>(null);
        private transient Transport.PacketPickler _pr;

        ALCheckpointHandle(Watermark aLowWatermark, Transport.Packet aCollect, AcceptorLearner anAl, Transport.PacketPickler aPickler) {
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
            _lastCollect = (Transport.Packet) _pr.unpickle(myBytes);
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
	
	private ALCheckpointHandle _lastCheckpoint;
	
    /* ********************************************************************************************
     *
     * Lifecycle, checkpointing and open/close
     *
     ******************************************************************************************** */

    public AcceptorLearner(LogStorage aStore, Common aCommon) {
        _storage = aStore;
        _localAddress = aCommon.getTransport().getLocalAddress();
        _common = aCommon;
        _lastCheckpoint = new ALCheckpointHandle(Watermark.INITIAL, _common.getLastCollect(), null, _common.getTransport().getPickler());        
    }

    /**
     * If this method fails be sure to invoke close regardless.
     *
     * @param aHandle
     * @throws Exception
     */
    public void open(CheckpointHandle aHandle) throws Exception {
        _storage.open();

        try {
            _common.setState(Common.FSMStates.RECOVERING);

            long myStartSeqNum = -1;
            
            if (! aHandle.equals(CheckpointHandle.NO_CHECKPOINT)) {
                if (aHandle instanceof ALCheckpointHandle)
                    myStartSeqNum = installCheckpoint((ALCheckpointHandle) aHandle);
                else
                    throw new IllegalArgumentException("Not a valid CheckpointHandle: " + aHandle);
            }

            try {
                new LogRangeProducer(myStartSeqNum, Long.MAX_VALUE, new Consumer() {
                    public void process(Transport.Packet aPacket, long aLogOffset) {
                        AcceptorLearner.this.process(aPacket, new ReplayWriter(aLogOffset), new RecoverySender());
                    }
                }, _storage, _common.getTransport().getPickler()).produce(0);
            } catch (Exception anE) {
                _logger.error("Failed to replay log", anE);
            }
        } finally {
            _common.setState(Common.FSMStates.ACTIVE);
        }
    }

    public void close() {
        try {
            /*
             * Allow for the fact we might be actively processing packets. Mark ourselves shutdown and drain...
             */
        	_common.setState(Common.FSMStates.SHUTDOWN);

            try {
                Thread.sleep(SHUTDOWN_PAUSE);
            } catch (Exception anE) {}

            synchronized(this) {
                _recoveryAlarm = null;
                _recoveryWindow = null;
                _common.resetLeader();
                _common.getRecoveryTrigger().reset();
            }

            _packetBuffer.clear();
            _cachedBegins.clear();

            _storage.close();

        } catch (Exception anE) {
            _logger.error("Failed to close storage", anE);
            throw new Error(anE);
        }
    }

    public CheckpointHandle newCheckpoint() {
        // Can't allow watermark and last collect to vary independently
        //
        synchronized (this) {
            return new ALCheckpointHandle(_common.getRecoveryTrigger().getLowWatermark(),
                    _common.getLastCollect(), this, _common.getTransport().getPickler());
        }
    }

    private void saved(ALCheckpointHandle aHandle) throws Exception {
        // If the checkpoint we're installing is newer...
        //
        synchronized(this) {
            if (aHandle.isNewerThan(_lastCheckpoint))
                _lastCheckpoint = aHandle;
            else
                return;
        }

        _storage.mark(aHandle.getLowWatermark().getLogOffset(), true);
    }

    private long installCheckpoint(ALCheckpointHandle aHandle) {
        _lastCheckpoint = aHandle;
        _common.setLastCollect(aHandle.getLastCollect());

        _logger.info("Checkpoint installed: " + aHandle.getLastCollect() + " @ " + aHandle.getLowWatermark());

        return _common.getRecoveryTrigger().install(aHandle.getLowWatermark());
    }

    /**
     * When an AL is out of date, call this method to bring it back into sync from a remotely sourced
     * checkpoint.
     *
     * @param aHandle obtained from the remote checkpoint.
     * @throws Exception
     */
    public boolean bringUpToDate(CheckpointHandle aHandle) throws Exception {
        if (! _common.testState(Common.FSMStates.OUT_OF_DATE))
            throw new IllegalStateException("Not out of date");

        if (! (aHandle instanceof ALCheckpointHandle))
            throw new IllegalArgumentException("Not a valid CheckpointHandle: " + aHandle);

        ALCheckpointHandle myHandle = (ALCheckpointHandle) aHandle;

        if (! myHandle.isNewerThan(_lastCheckpoint))
            return false;

        /*
         * If we're out of date, there will be no active timers and no activity on the log.
         * We should install the checkpoint and clear out all state associated with known instances
         * of Paxos. Additional state will be obtained from another AL via the lightweight recovery protocol.
         * Out of date means that the existing log has no useful data within it implying it can be discarded.
         */
        synchronized(this) {
            installCheckpoint(myHandle);
            
            // Write collect from our new checkpoint to log and use that as the starting point for replay.
            //
            _storage.mark(new LiveWriter().write(myHandle.getLastCollect(), false), true);

            _common.setState(Common.FSMStates.ACTIVE);
            _recoveryWindow = null;

            /*
             * We do not want to allow a leader to immediately over-rule us, make it work a bit,
             * otherwise we risk leader jitter. This ensures we get proper leader leasing as per
             * live packet processing.
             */
            _common.leaderAction();
            _common.signal(new VoteOutcome(VoteOutcome.Reason.UP_TO_DATE,
                    ((Collect) myHandle.getLastCollect().getMessage()).getSeqNum(), 
                    ((Collect) myHandle.getLastCollect().getMessage()).getRndNumber(),
            		Proposal.NO_VALUE, myHandle.getLastCollect().getSource()));
        }

        _packetBuffer.clear();
        _cachedBegins.clear();

        return true;
    }

    /* ********************************************************************************************
     *
     * Utility methods
     * 
     ******************************************************************************************** */

	public long getHeartbeatCount() {
		return _receivedHeartbeats.longValue();
	}

	public long getIgnoredCollectsCount() {
		return _ignoredCollects.longValue();
	}

    private void cacheBegin(Begin aBegin) {
        _cachedBegins.put(new Long(aBegin.getSeqNum()), aBegin);
    }

    private Begin expungeBegin(long aSeqNum) {
        return _cachedBegins.remove(new Long(aSeqNum));
    }

    /* ********************************************************************************************
     *
     * Core message processing
     *
     ******************************************************************************************** */

	/**
	 * @param aMessage
	 */
	public void messageReceived(Transport.Packet aPacket) {
        // If we're not processing packets because we're out of date or because we're shutting down
        //
        if ((_common.testState(Common.FSMStates.OUT_OF_DATE)) || (_common.testState(Common.FSMStates.SHUTDOWN)))
            return;

        PaxosMessage myMessage = aPacket.getMessage();
        long mySeqNum = myMessage.getSeqNum();
        Writer myWriter = new LiveWriter();

		if (_common.testState(Common.FSMStates.RECOVERING)) {
            switch (myMessage.getType()) {
                // If the packet is a recovery request, ignore it
                //
                case Operations.NEED : return;

                // If we're out of date, we need to get the user-code to find a checkpoint
                //
                case Operations.OUTOFDATE : {
                    synchronized(this) {
                        completedRecovery();
                        _common.setState(Common.FSMStates.OUT_OF_DATE);
                    }

                    // Signal with the node that pronounced us out of date - likely user code will get ckpt from there.
                    //
                    _common.signal(new VoteOutcome(VoteOutcome.Reason.OUT_OF_DATE, mySeqNum,
                            _common.getLeaderRndNum(),
                            new Proposal(), aPacket.getSource()));
                    return;
                }
            }
        }

        boolean myRecoveryInProgress = _common.testState(Common.FSMStates.RECOVERING);
        Need myWindow = _common.getRecoveryTrigger().shouldRecover(myMessage.getSeqNum(), _localAddress);

        if (myRecoveryInProgress) {
			// If the packet is for a sequence number above the recovery window - save it for later
			//
			if (mySeqNum > _recoveryWindow.getMaxSeq()) {
                try {
                    _packetBuffer.put(aPacket);
                } catch (InterruptedException anIE) {
                    _logger.error("Coudn't queue to PacketBuffer", anIE);
                    throw new RuntimeException("Serious recovery failure", anIE);
                }

				
			// If the packet is for a sequence number within the window, process it now for catchup
			//
			} else if (mySeqNum > _recoveryWindow.getMinSeq()) {
				process(aPacket, myWriter, new RecoverySender());
				
				/*
				 *  If the low watermark is now at the top of the recovery window, we're ready to resume once we've
				 *  streamed through the packet buffer
				 */
				if (_common.getRecoveryTrigger().getLowWatermark().getSeqNum() ==
                        _recoveryWindow.getMaxSeq()) {
					synchronized(this) {
						for (Transport.Packet myReplayPacket : _packetBuffer) {

							// May be excessive gaps in buffer, catch that, exit early, recovery will trigger again
							//
							if (_common.getRecoveryTrigger().shouldRecover(myReplayPacket.getMessage().getSeqNum(),
                                    _localAddress) != null)
								break;

							process(myReplayPacket, myWriter, new RecoverySender());
						}
						
                        completedRecovery();
                        _common.setState(Common.FSMStates.ACTIVE);
					}
				}
			} else {			
				// Packet is below recovery window, ignore it.
				//
			}
		} else {
			if (myWindow != null) {
				/*
				 * Ideally we catch up to the end of a complete instance but we don't have to. The AL state machine
				 * will simply discard some packets and then trigger another recovery.
				 *
				 * It's possible that, for example, we miss a collect but catch a begin and a success. In such a
				 * case we'd drop the messages and then issue another recovery. In the case of the multi-paxos
				 * optimisation a similar thing would happen should a new leader have just taken over. Otherwise,
				 * we'll likely recover the collect from another AL and thus apply all the packets since successfully.
				 */

                // Must store up the packet which triggered recovery for later replay
                //
                try {
                    _packetBuffer.put(aPacket);
                } catch (InterruptedException anIE) {
                    _logger.error("Coudn't queue to PacketBuffer", anIE);
                    throw new RuntimeException("Serious recovery failure", anIE);
                }

                synchronized(this) {
					/*
					 * Ask a node to bring us up to speed. Note that this node mightn't have all the records we need.
					 * If that's the case, it won't stream at all or will only stream some part of our recovery
					 * window. As the recovery watchdog measures progress through the recovery window, a partial
					 * recovery or no recovery will be noticed and we'll ask a new random node to bring us up to speed.
					 */
					new LiveSender().send(myWindow, _common.getFD().getRandomMember(_localAddress));

					// Declare recovery active - which stops this AL emitting responses
					//
                    _common.setState(Common.FSMStates.RECOVERING);
                    _recoveryWindow = myWindow;

                    // Startup recovery watchdog
                    //
                    reschedule();
				}
			} else
				process(aPacket, myWriter, new LiveSender());
		}
	}

    /* ********************************************************************************************
     *
     * Recovery logic - watchdogs, catchup and such
     *
     ******************************************************************************************** */

    private class Watchdog extends TimerTask {
        private final Watermark _past = _common.getRecoveryTrigger().getLowWatermark();

        public void run() {
            if (! _common.testState(Common.FSMStates.RECOVERING)) {
                // Someone else will do cleanup
                //
                return;
            }

            /*
             * If the watermark has advanced since we were started, recovery made some progress so we'll schedule a
             * future check otherwise fail.
             */
            if (! _past.equals(_common.getRecoveryTrigger().getLowWatermark())) {
                _logger.debug("Recovery is progressing, " + _localAddress);

                reschedule();
            } else {
                _logger.warn("Recovery is NOT progressing - terminate, " + _localAddress);

                terminateRecovery();
                _common.setState(Common.FSMStates.ACTIVE);
            }
        }
    }

    private void reschedule() {
        _logger.debug("Rescheduling, " + _localAddress);

        synchronized(this) {
            _recoveryAlarm = new Watchdog();
            _common.getWatchdog().schedule(_recoveryAlarm, calculateRecoveryGracePeriod());
        }
    }

    public void setRecoveryGracePeriod(long aPeriod) {
        synchronized(this) {
            _gracePeriod = aPeriod;
        }
    }

    private long calculateRecoveryGracePeriod() {
        synchronized(this) {
            return _gracePeriod;
        }
    }

    private void terminateRecovery() {
        _logger.debug("Recovery terminate, " + _localAddress);

        /*
         * This will cause the AL to re-enter recovery when another packet is received and thus a new
         * NEED will be issued. Eventually we'll get too out-of-date or updated
         */
        synchronized(this) {
            _recoveryAlarm = null;
            postRecovery();
        }
    }

    private void completedRecovery() {
        _logger.info("Recovery complete, " + _localAddress);

        synchronized(this) {
            if (_recoveryAlarm != null) {
                _recoveryAlarm.cancel();
                _common.getWatchdog().purge();
                _recoveryAlarm = null;
            }

            postRecovery();
        }
    }

    private void postRecovery() {
        synchronized(this) {
            _recoveryWindow = null;
        }

        _packetBuffer.clear();
    }

    /* ********************************************************************************************
     *
     * Core state machine
     *
     ******************************************************************************************** */

	/**
	 * @param aMessage is the message to process
	 * @return is any message to send
	 */
	private void process(Transport.Packet aPacket, Writer aWriter, Sender aSender) {
        PaxosMessage myMessage = aPacket.getMessage();
		InetSocketAddress myNodeId = aPacket.getSource();
		long mySeqNum = myMessage.getSeqNum();

		_logger.info("AL: got " + myNodeId + " " + myMessage + ", " + _localAddress);

		switch (myMessage.getType()) {
            case Operations.NEED : {
                final Need myNeed = (Need) myMessage;

                /*
                 * Make sure we can dispatch the recovery request - if the requested sequence number is less than
                 * our checkpoint low watermark we don't have the log available and thus we tell the AL it's out of
                 * date. Since Paxos will tend to keep replica's mostly in sync there's no need to see if other
                 * nodes pronounce out of date as likely they will, eventually (allowing for network instabilities).
                 */
                if (myNeed.getMinSeq() < _lastCheckpoint.getLowWatermark().getSeqNum()) {
                    _common.getTransport().send(new OutOfDate(), aPacket.getSource());

                } else if (myNeed.getMaxSeq() <= _common.getRecoveryTrigger().getLowWatermark().getSeqNum()) {
                    _logger.debug("Running streamer: " + _localAddress);


                    _common.getTransport().connectTo(aPacket.getSource(),
                            new Transport.ConnectionHandler() {
                                public void connected(Stream aStream) {
                                    new RemoteStreamer(myNeed, aStream).start();
                                }
                            });
                }

                break;
            }
			
			case Operations.COLLECT: {
				Collect myCollect = (Collect) myMessage;

				if (!_common.amAccepting(aPacket)) {
					_ignoredCollects.incrementAndGet();

					_logger.warn("AL:Not accepting: " + myCollect + ", "
							+ getIgnoredCollectsCount() + ", " + _localAddress);
					return;
				}

				// If the collect supercedes our previous collect save it, return last proposal etc
				//
				if (_common.supercedes(aPacket)) {
					aWriter.write(aPacket, true);
                    
                    aSender.send(constructLast(mySeqNum), myNodeId);

					/*
					 * If the collect comes from the current leader (has same rnd
					 * and node), we apply the multi-paxos optimisation, no need to
					 * save to disk, just respond with last proposal etc
					 */
				} else if (_common.sameLeader(aPacket)) {
                    aSender.send(constructLast(mySeqNum), myNodeId);

				} else {
					// Another collect has already arrived with a higher priority, tell the proposer it has competition
					//
                    aSender.send(new OldRound(_common.getRecoveryTrigger().getLowWatermark().getSeqNum(),
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
                    cacheBegin(myBegin);
                    
					aWriter.write(aPacket, true);

					aSender.send(new Accept(mySeqNum, _common.getLeaderRndNum()), myNodeId);
				} else if (_common.precedes(aPacket)) {
					// New collect was received since the collect for this begin,
					// tell the proposer it's got competition
					//
					aSender.send(new OldRound(_common.getRecoveryTrigger().getLowWatermark().getSeqNum(),
                            _common.getLeaderAddress(), _common.getLeaderRndNum()), myNodeId);
				} else {
					/*
					 * Quiet, didn't see the collect, leader hasn't accounted for
					 * our values, it hasn't seen our last and we're likely out of sync with the majority
					 */
					_logger.warn("AL:Missed collect, going silent: " + mySeqNum
							+ " [ " + myBegin.getRndNumber() + " ], " + _localAddress);
				}
				
				break;
			}

			case Operations.SUCCESS: {
				Success mySuccess = (Success) myMessage;

                _common.leaderAction();

				if (mySeqNum <= _common.getRecoveryTrigger().getLowWatermark().getSeqNum()) {
					_logger.debug("AL:Discarded known value: " + mySeqNum + ", " + _localAddress);
				} else {
                    Begin myBegin = expungeBegin(mySeqNum);

                    if ((myBegin == null) || (myBegin.getRndNumber() != mySuccess.getRndNum())) {
                        // We never saw the appropriate begin
                        //
                        _logger.debug("AL: Discarding success: " + myBegin + ", " + mySuccess +
                                ", " + _localAddress);
                    } else {
                        // Record the success even if it's the heartbeat so there are no gaps in the Paxos sequence
                        //
                        long myLogOffset = aWriter.write(aPacket, true);

                        _common.getRecoveryTrigger().completed(new Watermark(mySeqNum, myLogOffset));

                        if (myBegin.getConsolidatedValue().equals(LeaderFactory.HEARTBEAT)) {
                            _receivedHeartbeats.incrementAndGet();

                            _logger.debug("AL: discarded heartbeat: "
                                    + System.currentTimeMillis() + ", "
                                    + getHeartbeatCount() + ", " + _localAddress);
                        } else {
                            _logger.info("AL:Learnt value: " + mySeqNum + ", " + _localAddress);

                            _common.signal(new VoteOutcome(VoteOutcome.Reason.DECISION, mySeqNum,
                                    _common.getLeaderRndNum(),
                                    myBegin.getConsolidatedValue(), aPacket.getSource()));
                        }
                    }
				}

				break;
			}

			default:
				throw new RuntimeException("Unexpected message" + ", " + _localAddress);
		}
	}

	private PaxosMessage constructLast(long aSeqNum) {
		Watermark myLow = _common.getRecoveryTrigger().getLowWatermark();
		
		/*
		 * If the sequence number is less than the current low watermark, we've got to check through the log file for
		 * the value otherwise if it's present, it will be since the low watermark offset.
		 */
		Begin myState;
		
		try {
			if ((myLow.equals(Watermark.INITIAL)) || (aSeqNum <= myLow.getSeqNum())) {
				myState = new StateFinder(aSeqNum, 0).getState();
			} else 
				myState = new StateFinder(aSeqNum, myLow.getLogOffset()).getState();
		} catch (Exception anE) {
			_logger.error("Failed to replay log" + ", " + _localAddress, anE);
			throw new RuntimeException("Failed to replay log" + ", " + _localAddress, anE);
		}
		
		if (myState != null) {
            return new Last(aSeqNum, myLow.getSeqNum(), myState.getRndNumber(), myState.getConsolidatedValue());
		} else {
            /*
             * If we've got here, we've not found the sequence number in our log. If the sequence number is less
             * than or equal our low watermark then we've checkpointed and gc'd the information into a snapshot. This
             * means the leader is running sufficiently far behind that it mightn't be able to drive the current
             * sequence number to completion, we must tell it.
             */
            if (aSeqNum <= myLow.getSeqNum())
                return new OldRound(aSeqNum, _common.getLeaderAddress(),
                        _common.getLeaderRndNum());
            else
                return new Last(aSeqNum, myLow.getSeqNum(),
                    Long.MIN_VALUE, Proposal.NO_VALUE);            
		}
	}

    interface Sender {
        public void send(PaxosMessage aMessage, InetSocketAddress aNodeId);
    }
    
    class LiveSender implements Sender {
        public void send(PaxosMessage aMessage, InetSocketAddress aNodeId) {
            _logger.debug("AL sending: " + aMessage);
            _common.getTransport().send(aMessage, aNodeId);
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
                _logger.error("AL: cannot log: " + System.currentTimeMillis() + ", " + _localAddress, anE);
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
            new LogRangeProducer(aSeqNum - 1, aSeqNum, this, _storage, _common.getTransport().getPickler()).produce(aLogOffset);
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
        private Need _need;
        private Stream _stream;

        RemoteStreamer(Need aNeed, Stream aStream) {
            _need = aNeed;
            _stream = aStream;
        }

        public void run() {
            _logger.info("RemoteStreamer starting, " + _localAddress);

            try {
                new LogRangeProducer(_need.getMinSeq(), _need.getMaxSeq(), this, _storage, _common.getTransport().getPickler()).produce(0);
            } catch (Exception anE) {
                _logger.error("Failed to replay log", anE);
            } finally {
                _stream.close();
            }
        }

        public void process(Transport.Packet aPacket, long aLogOffset) {
            _logger.debug("Streaming: " + aPacket.getMessage());
            _stream.sendRaw(aPacket);
        }
    }
}
