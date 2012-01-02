package org.dancres.paxos.impl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.messages.*;
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
 * @todo Send out of date to a NEED'ing AL - this message would be generated when
 * the low end of the recovery window is below the last checkpoint's sequence number.
 * <code>bringUpToDate</code> should reject any checkpoint that doesn't contain a
 * low watermark greater than the one currently known by this AL. This allows user
 * code to source checkpoints from a list of nodes and be informed if the checkpoints
 * aren't good enough so eventually a useful one is found. An optimisation would
 * involve the AL generating the OutOfDate message to include it's last checkpoint
 * low watermark. This watermark would then be passed in the OUT_OF_DATE event to
 * user code which could then pass it across to targets AL's so they can validate
 * they have a useful checkpoint before it's streamed back by the user code.
 * 
 * @author dan
 */
public class AcceptorLearner {
    private static final long DEFAULT_RECOVERY_GRACE_PERIOD = 30 * 1000;

    // Pause should be less than DEFAULT_GRACE_PERIOD
    //
    private static final long SHUTDOWN_PAUSE = 1 * 1000;

	private static Logger _logger = LoggerFactory.getLogger(AcceptorLearner.class);

	/**
	 * Statistic that tracks the number of Collects this AcceptorLearner ignored
	 * from competing leaders within DEFAULT_LEASE ms of activity from the
	 * current leader.
	 */
	private AtomicLong _ignoredCollects = new AtomicLong();
	private AtomicLong _receivedHeartbeats = new AtomicLong();

    private long _gracePeriod = DEFAULT_RECOVERY_GRACE_PERIOD;

    private TimerTask _recoveryAlarm = null;

	private List<PaxosMessage> _packetBuffer = new LinkedList<PaxosMessage>();

	/**
     * Tracks the last collect we accepted and thus represents our view of the current leader for instances.
     * Other leaders wishing to take over must present a round number greater than this and after expiry of the
     * lease for the previous leader. The last collect is also used to validate begins and ensure we don't accept
     * any from other leaders whilst we're not in sync with the majority.
	 */
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
    private final Map<Long, Begin> _cachedBegins = new HashMap<Long, Begin>();

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
        private long _seqNum;
        private long _logOffset;

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
    	static final ALCheckpointHandle NO_CHECKPOINT = new ALCheckpointHandle(Watermark.INITIAL, Collect.INITIAL, null);
    	
        private transient Watermark _lowWatermark;
        private transient Collect _lastCollect;
        private transient AcceptorLearner _al;

        ALCheckpointHandle(Watermark aLowWatermark, Collect aCollect, AcceptorLearner anAl) {
            _lowWatermark = aLowWatermark;
            _lastCollect = aCollect;
            _al = anAl;
        }

        private void readObject(ObjectInputStream aStream) throws IOException, ClassNotFoundException {
            aStream.defaultReadObject();
            _lowWatermark = new Watermark(aStream.readLong(), aStream.readLong());

            byte[] myBytes = new byte[aStream.readInt()];
            aStream.readFully(myBytes);
            _lastCollect = (Collect) Codecs.decode(myBytes);
        }

        private void writeObject(ObjectOutputStream aStream) throws IOException {
            aStream.defaultWriteObject();
            aStream.writeLong(_lowWatermark.getSeqNum());
            aStream.writeLong(_lowWatermark.getLogOffset());

            byte[] myCollect = Codecs.encode(_lastCollect);
            aStream.writeInt(myCollect.length);
            aStream.write(Codecs.encode(_lastCollect));
        }

        public void saved() throws Exception {
            _al.saved(this);
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

        Collect getLastCollect() {
            return _lastCollect;
        }
        
        public boolean equals(Object anObject) {
        	if (anObject instanceof ALCheckpointHandle) {
        		ALCheckpointHandle myOther = (ALCheckpointHandle) anObject;
        		
        		return ((_lowWatermark.equals(myOther._lowWatermark)) && (_lastCollect.equals(myOther._lastCollect)));
        	}
        	
        	return false;
        }
    }
	
	private ALCheckpointHandle _lastCheckpoint = ALCheckpointHandle.NO_CHECKPOINT;
	
    /* ********************************************************************************************
     *
     * Lifecycle, checkpointing and open/close
     *
     ******************************************************************************************** */

    public AcceptorLearner(LogStorage aStore, Common aCommon) {
        _storage = aStore;
        _localAddress = aCommon.getTransport().getLocalAddress();
        _common = aCommon;
    }

    public CheckpointHandle newCheckpoint() {
        // Can't allow watermark and last collect to vary independently
        //
        synchronized (this) {
            return new ALCheckpointHandle(_common.getRecoveryTrigger().getLowWatermark(),
                    _common.getLastCollect(), this);
        }
    }

    private void saved(ALCheckpointHandle aHandle) throws Exception {
    	if (aHandle.getLowWatermark().equals(Watermark.INITIAL))
    		return;
    	
    	// If the checkpoint we're installing is newer...
    	//
    	synchronized(this) {
    		if (_lastCheckpoint.getLowWatermark().getSeqNum() < aHandle.getLowWatermark().getSeqNum())
    			_lastCheckpoint = aHandle;
    		else
    			return;
    	}
    	
		_storage.mark(aHandle.getLowWatermark().getLogOffset(), true);
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
            _common.setRecoveryWindow(new Need(-1, Long.MAX_VALUE, _localAddress));

            if (aHandle.equals(CheckpointHandle.NO_CHECKPOINT)) {
                // Restore logs from the beginning
                //
                _logger.info("No checkpoint - replay from the beginning");

                try {
					new LogRangeProducer(-1, Long.MAX_VALUE, new Consumer() {
						public void process(PaxosMessage aMsg) {
							AcceptorLearner.this.process(aMsg);
						}
					}, _storage).produce(0);
                } catch (Exception anE) {
                    _logger.error("Failed to replay log", anE);
                }

            } else if (aHandle instanceof ALCheckpointHandle) {
                ALCheckpointHandle myHandle = (ALCheckpointHandle) aHandle;

                try {
					new LogRangeProducer(installCheckpoint(myHandle),
							Long.MAX_VALUE, new Consumer() {
								public void process(PaxosMessage aMsg) {
									AcceptorLearner.this.process(aMsg);
								}
							}, _storage).produce(0);
                } catch (Exception anE) {
                    _logger.error("Failed to replay log", anE);
                }

            } else
                throw new IllegalArgumentException("Not a valid CheckpointHandle: " + aHandle);
        } finally {
            _common.clearRecoveryWindow();
        }
    }

    public void close() {
        try {
            /*
             * Allow for the fact we might be actively processing packets - Mark ourselves out of date so we
             * cease processing, then clean up.
             */
        	_common.setSuspended(true);

            try {
                Thread.sleep(SHUTDOWN_PAUSE);
            } catch (Exception anE) {}

            synchronized(this) {
                _recoveryAlarm = null;
                _common.clearRecoveryWindow();
                _packetBuffer.clear();
                _cachedBegins.clear();
                _common.resetLeader();
                _common.getRecoveryTrigger().reset();
            }

            _storage.close();

        } catch (Exception anE) {
            _logger.error("Failed to close logger", anE);
            throw new Error(anE);
        }
    }

    private long installCheckpoint(ALCheckpointHandle aHandle) {
        _lastCheckpoint = aHandle;
        _common.setLastCollect(aHandle.getLastCollect());

        _logger.info("Checkpoint installed: " + _common.getLastCollect() + " @ " + aHandle.getLowWatermark());

        return _common.getRecoveryTrigger().install(aHandle.getLowWatermark());
    }

    /**
     * When an AL is out of date, call this method to bring it back into sync from a remotely sourced
     * checkpoint.
     *
     * @todo Leader jitter, really?
     *
     * @param aHandle obtained from the remote checkpoint.
     * @throws Exception
     */
    public void bringUpToDate(CheckpointHandle aHandle) throws Exception {
        if (! _common.isSuspended())
            throw new IllegalStateException("Not suspended");

        if (! (aHandle instanceof ALCheckpointHandle))
            throw new IllegalArgumentException("Not a valid CheckpointHandle: " + aHandle);

        ALCheckpointHandle myHandle = (ALCheckpointHandle) aHandle;

        if (myHandle.equals(CheckpointHandle.NO_CHECKPOINT))
            throw new IllegalArgumentException("Cannot update to initial checkpoint: " + myHandle);

        /*
         * If we're out of date, there will be no active timers and no activity on the log.
         * We should install the checkpoint and clear out all state associated with known instances
         * of Paxos. Additional state will be obtained from another AL via the lightweight recovery protocol.
         * Out of date means that the existing log has no useful data within it implying it can be discarded.
         */
        synchronized(this) {
            installCheckpoint(myHandle);
            
            // Write the collect we've just found in our new checkpoint and use that as the starting point
            // for the log
            //
            _storage.mark(write(myHandle.getLastCollect(), false), true);

            _common.setSuspended(false);

            _common.clearRecoveryWindow();
            _packetBuffer.clear();
            _cachedBegins.clear();

            /*
             * We do not want to allow a leader to immediately over-rule us, make it work a bit,
             * otherwise we risk leader jitter
             */
            _common.leaderAction();
            _common.signal(new VoteOutcome(VoteOutcome.Reason.UP_TO_DATE,
                    myHandle.getLastCollect().getSeqNum(), myHandle.getLastCollect().getRndNumber(),
            		Proposal.NO_VALUE, myHandle.getLastCollect().getNodeId()));
        }
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
        synchronized(this) {
            _cachedBegins.put(new Long(aBegin.getSeqNum()), aBegin);
        }
    }

    private Begin expungeBegin(long aSeqNum) {
        synchronized(this) {
            return _cachedBegins.remove(new Long(aSeqNum));
        }
    }

    /* ********************************************************************************************
     *
     * Core message processing
     *
     ******************************************************************************************** */

	/**
	 * @param aMessage
	 */
	public void messageReceived(PaxosMessage aMessage) {
		long mySeqNum = aMessage.getSeqNum();

        // If we're not processing packets (perhaps because we're out of date or because we're shutting down)...
        //
        if (_common.isSuspended())
            return;

		if (_common.isRecovering()) {
			// If the packet is a recovery request, ignore it
			//
			if (aMessage.getType() == Operations.NEED)
				return;

            // If the packet is out of date, we need a state restore
            //
            if (aMessage.getType() == Operations.OUTOFDATE) {
                synchronized(this) {
                	_common.setSuspended(true);
                    completedRecovery();
                }

                _common.signal(new VoteOutcome(VoteOutcome.Reason.OUT_OF_DATE, mySeqNum,
                        _common.getLastCollect().getRndNumber(),
                        new Proposal(), _common.getLastCollect().getNodeId()));
                return;
            }

			// If the packet is for a sequence number above the recovery window - save it for later
			//
			if (mySeqNum > _common.getRecoveryWindow().getMaxSeq()) {
				synchronized(this) {
					_packetBuffer.add(aMessage);
				}
				
			// If the packet is for a sequence number within the window, process it now for catchup
			//
			} else if (mySeqNum > _common.getRecoveryWindow().getMinSeq()) {
				process(aMessage);
				
				/*
				 *  If the low watermark is now at the top of the recovery window, we're ready to resume once we've
				 *  streamed through the packet buffer
				 */
				if (_common.getRecoveryTrigger().getLowWatermark().getSeqNum() ==
                        _common.getRecoveryWindow().getMaxSeq()) {
					synchronized(this) {
						Iterator<PaxosMessage> myPackets = _packetBuffer.iterator();
						
						while (myPackets.hasNext()) {
							PaxosMessage myMessage = myPackets.next();
							
							// May be excessive gaps in buffer, catch that, exit early, recovery will trigger again
							//
							if (_common.getRecoveryTrigger().shouldRecover(myMessage.getSeqNum(), _localAddress) != null)
								break;

							process(myMessage);
						}
						
                        completedRecovery();
					}
				}
			} else {			
				// Packet is below recovery window, ignore it.
				//
			}
		} else {
			Need myWindow = _common.getRecoveryTrigger().shouldRecover(aMessage.getSeqNum(), _localAddress);

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
				synchronized(this) {
					// Must store up the packet which triggered recovery for later replay
					//
					_packetBuffer.add(aMessage);
					
					/*
					 * Ask a node to bring us up to speed. Note that this node mightn't have all the records we need.
					 * If that's the case, it won't stream at all or will only stream some part of our recovery
					 * window. As the recovery watchdog measures progress through the recovery window, a partial
					 * recovery or no recovery will be noticed and we'll ask a new random node to bring us up to speed.
					 */
					send(myWindow, _common.getFD().getRandomMember(_localAddress));

					// Declare recovery active - which stops this AL emitting responses
					//
                    _common.setRecoveryWindow(myWindow);

                    // Startup recovery watchdog
                    //
                    reschedule();
				}
			} else
				process(aMessage);
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
            if (! _common.isRecovering()) {
                // Someone else will do cleanup
                //
                return;
            }

            /*
             * If the watermark has advanced since we were started recovery made some progress so we'll schedule a
             * future check otherwise fail.
             */
            if (! _past.equals(_common.getRecoveryTrigger().getLowWatermark())) {
                _logger.info("Recovery is progressing, " + _localAddress);

                reschedule();
            } else {
                _logger.info("Recovery is NOT progressing - terminate, " + _localAddress);

                terminateRecovery();
            }
        }
    }

    private void reschedule() {
        _logger.info("Rescheduling, " + _localAddress);

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
        _logger.info("Recovery terminate, " + _localAddress);

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
            _common.clearRecoveryWindow();
            _packetBuffer.clear();
        }
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
	private void process(PaxosMessage aMessage) {
		InetSocketAddress myNodeId = aMessage.getNodeId();
		long mySeqNum = aMessage.getSeqNum();

		_logger.info("AL: got " + aMessage + ", " + _localAddress);

		switch (aMessage.getType()) {
			case Operations.NEED : {
				Need myNeed = (Need) aMessage;
				
				// Make sure we can dispatch the recovery request
				//
				if (myNeed.getMinSeq() < _lastCheckpoint.getLowWatermark().getSeqNum())
					_common.getTransport().send(new OutOfDate(_localAddress), myNeed.getNodeId());
				
				if (myNeed.getMaxSeq() <= _common.getRecoveryTrigger().getLowWatermark().getSeqNum()) {
					_logger.info("Running streamer: " + _localAddress);
					
					new RemoteStreamer(myNeed).run();
				}
				
				break;
			}
			
			case Operations.COLLECT: {
				Collect myCollect = (Collect) aMessage;

				if (!_common.amAccepting(myCollect)) {
					_ignoredCollects.incrementAndGet();

					_logger.info("AL:Not accepting: " + myCollect + ", "
							+ getIgnoredCollectsCount() + ", " + _localAddress);
					return;
				}

				// If the collect supercedes our previous collect save it to disk,
				// return last proposal etc
				//
				if (_common.supercedes(myCollect)) {
					write(aMessage, true);
                    
                    send(constructLast(mySeqNum), myNodeId);

					/*
					 * If the collect comes from the current leader (has same rnd
					 * and node), we apply the multi-paxos optimisation, no need to
					 * save to disk, just respond with last proposal etc
					 */
				} else if (myCollect.sameLeader(_common.getLastCollect())) {
                    send(constructLast(mySeqNum), myNodeId);

				} else {
					// Another collect has already arrived with a higher priority,
					// tell the proposer it has competition
					//
					Collect myLastCollect = _common.getLastCollect();

					send(new OldRound(_common.getRecoveryTrigger().getLowWatermark().getSeqNum(),
                            myLastCollect.getNodeId(), myLastCollect.getRndNumber(), _localAddress), myNodeId);
				}
				
				break;
			}

			case Operations.BEGIN: {
				Begin myBegin = (Begin) aMessage;

				// If the begin matches the last round of a collect we're fine
				//
				if (myBegin.originates(_common.getLastCollect())) {
					_common.leaderAction();
                    cacheBegin(myBegin);
                    
					write(aMessage, true);

					send(new Accept(mySeqNum, _common.getLastCollect().getRndNumber(), 
							_localAddress), myNodeId);
				} else if (myBegin.precedes(_common.getLastCollect())) {
					// New collect was received since the collect for this begin,
					// tell the proposer it's got competition
					//
					Collect myLastCollect = _common.getLastCollect();

					send(new OldRound(_common.getRecoveryTrigger().getLowWatermark().getSeqNum(),
                            myLastCollect.getNodeId(), myLastCollect.getRndNumber(), _localAddress), myNodeId);
				} else {
					// Quiet, didn't see the collect, leader hasn't accounted for
					// our values, it hasn't seen our last and we're likely out of sync with the majority
					//
					_logger.info("AL:Missed collect, going silent: " + mySeqNum
							+ " [ " + myBegin.getRndNumber() + " ], " + _localAddress);
				}
				
				break;
			}

			case Operations.SUCCESS: {
				Success mySuccess = (Success) aMessage;

                _common.leaderAction();

				if (mySeqNum <= _common.getRecoveryTrigger().getLowWatermark().getSeqNum()) {
					_logger.info("AL:Discarded known value: " + mySeqNum + ", " + _localAddress);
				} else {
                    Begin myBegin = expungeBegin(mySeqNum);

                    if ((myBegin == null) || (myBegin.getRndNumber() != mySuccess.getRndNum())) {
                        // We never saw the appropriate begin
                        //
                        _logger.info("AL: Discarding success: " + myBegin + ", " + mySuccess +
                                ", " + _localAddress);
                    } else {
                        // Always record the success even if it's the heartbeat so there are
                        // no gaps in the Paxos sequence
                        //
                        long myLogOffset = write(aMessage, true);

                        _common.getRecoveryTrigger().completed(new Watermark(mySeqNum, myLogOffset));

                        if (myBegin.getConsolidatedValue().equals(LeaderFactory.HEARTBEAT)) {
                            _receivedHeartbeats.incrementAndGet();

                            _logger.info("AL: discarded heartbeat: "
                                    + System.currentTimeMillis() + ", "
                                    + getHeartbeatCount() + ", " + _localAddress);
                        } else {
                            _logger.info("AL:Learnt value: " + mySeqNum + ", " + _localAddress);

                            _common.signal(new VoteOutcome(VoteOutcome.Reason.DECISION, mySeqNum,
                                    _common.getLastCollect().getRndNumber(),
                                    myBegin.getConsolidatedValue(), myBegin.getNodeId()));
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
		Instance myState;
		
		try {
			if ((myLow.equals(Watermark.INITIAL)) || (aSeqNum <= myLow.getSeqNum())) {
				myState = new StateFinder(aSeqNum, 0).getState();
			} else 
				myState = new StateFinder(aSeqNum, myLow.getLogOffset()).getState();
		} catch (Exception anE) {
			_logger.error("Failed to replay log" + ", " + _localAddress, anE);
			throw new RuntimeException("Failed to replay log" + ", " + _localAddress, anE);
		}
		
		Begin myBegin = myState.getLastValue();
		
		if (myBegin != null) {
            return new Last(aSeqNum, myLow.getSeqNum(), myBegin.getRndNumber(), myBegin.getConsolidatedValue(),
                    _localAddress);
		} else {
            /*
             * If we've got here, we've not found the sequence number in our log. If the sequence number is less
             * than or equal our low watermark then we've checkpointed and gc'd the information into a snapshot. This
             * means the leader is running sufficiently far behind that it mightn't be able to drive the current
             * sequence number to completion, we must tell it.
             */
            if (aSeqNum <= _common.getRecoveryTrigger().getLowWatermark().getSeqNum())
                return new OldRound(aSeqNum, _common.getLastCollect().getNodeId(),
                        _common.getLastCollect().getRndNumber(), _localAddress);
            else
                return new Last(aSeqNum, _common.getRecoveryTrigger().getLowWatermark().getSeqNum(),
                    Long.MIN_VALUE, Proposal.NO_VALUE, _localAddress);            
		}
	}

    private void send(PaxosMessage aMessage, InetSocketAddress aNodeId) {
        // Stay silent during recovery to avoid producing out-of-date responses
        //
        if (! _common.isRecovering()) {
            _logger.info("AL sending: " + aMessage);
            _common.getTransport().send(aMessage, aNodeId);
        }
    }

    /* ********************************************************************************************
     *
     * Operation log handling
     *
     ******************************************************************************************** */

    private long write(PaxosMessage aMessage, boolean aForceRequired) {
        try {
            return _storage.put(Codecs.encode(aMessage), aForceRequired);
        } catch (Exception anE) {
            _logger.error("AL: cannot log: " + System.currentTimeMillis() + ", " + _localAddress, anE);
            throw new RuntimeException(anE);
        }
    }

    private static class Instance {
        private Begin _lastBegin;

        void add(PaxosMessage aMessage) {
            switch(aMessage.getType()) {
                case Operations.BEGIN : {

                    Begin myBegin = (Begin) aMessage;

                    if (_lastBegin == null) {
                        _lastBegin = myBegin;
                    } else if (myBegin.getRndNumber() > _lastBegin.getRndNumber()) {
                        _lastBegin = myBegin;
                    }

                    break;
                }

                default : { // Do nothing
                }
            }
        }

        Begin getLastValue() {
            return _lastBegin;
        }

        public String toString() {
            return "LoggedInstance: " + _lastBegin.getSeqNum() + " " + _lastBegin;
        }
    }

    /**
     * Used to locate the recorded state of a specified instance of Paxos.
     */
    private class StateFinder implements Consumer {
        private Instance _state = new Instance();

        StateFinder(long aSeqNum, long aLogOffset) throws Exception {
            new LogRangeProducer(aSeqNum - 1, aSeqNum, this, _storage).produce(aLogOffset);
        }

        Instance getState() {
            return _state;
        }

        public void process(PaxosMessage aMessage) {
            // dump("Read:", aRecord);

            // All records we write are leader messages and they all have no length
            //
            _state.add(aMessage);
        }
    }


    /**
     * Recovers all state since a particular instance of Paxos up to and including a specified maximum instance
     * and dispatches them to a particular remote node.
     */
    private class RemoteStreamer extends Thread implements Consumer {
        private Need _need;
        private Stream _stream;

        RemoteStreamer(Need aNeed) {
            _need = aNeed;
        }

        public void run() {
            _logger.info("RemoteStreamer starting, " + _localAddress);

            _stream = _common.getTransport().connectTo(_need.getNodeId());

            // Check we got a connection
            //
            if (_stream == null) {
                _logger.warn("RemoteStreamer couldn't connect: " + _need.getNodeId());
                return;
            }

            try {
                new LogRangeProducer(_need.getMinSeq(), _need.getMaxSeq(), this, _storage).produce(0);
            } catch (Exception anE) {
                _logger.error("Failed to replay log", anE);
            }

            _stream.close();
        }

        public void process(PaxosMessage aMsg) {
            _logger.debug("Streaming: " + aMsg);
            _stream.send(aMsg);
        }
    }
}
