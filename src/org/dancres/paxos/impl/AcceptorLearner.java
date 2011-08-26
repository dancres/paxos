package org.dancres.paxos.impl;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.dancres.paxos.ConsolidatedValue;
import org.dancres.paxos.Event;
import org.dancres.paxos.Paxos;
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
 * @author dan
 */
public class AcceptorLearner {
    private static final long DEFAULT_GRACE_PERIOD = 30 * 1000;

    // Pause should be less than DEFAULT_GRACE_PERIOD
    //
    private static final long SHUTDOWN_PAUSE = 1 * 1000;

    public static final long UNKNOWN_SEQ = -1;
    
	public static final ConsolidatedValue HEARTBEAT = new ConsolidatedValue("heartbeat",
			"org.dancres.paxos.Heartbeat".getBytes());

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

    private long _gracePeriod = DEFAULT_GRACE_PERIOD;

    private final Timer _watchdog = new Timer("AcceptorLearner timers");
    private TimerTask _recoveryAlarm = null;

    private OutOfDate _outOfDate = null;

    /*
     * If the sequence number of the collect we're seeing is for a sequence number >
     * lwm + 1, we've missed some packets. Recovery range r is lwm < r
     * <= x (where x = currentMessage.seqNum) so that the round we're
     * seeing right now is complete and we need save packets only after
     * that point.
     */
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

	private RecoveryWindow _recoveryWindow = null;
	private List<PaxosMessage> _packetBuffer = new LinkedList<PaxosMessage>();

	/**
     * Tracks the last collect we accepted and thus represents our view of the current leader for instances.
     * Other leaders wishing to take over must present a round number greater than this and after expiry of the
     * lease for the previous leader. The last collect is also used to validate begins and ensure we don't accept
     * any from other leaders whilst we're not in sync with the majority.
	 */
	private final LogStorage _storage;
	private final Transport _transport;
	private final FailureDetector _fd;
	
	private final long _leaderLease;
	private final List<Paxos.Listener> _listeners = new ArrayList<Paxos.Listener>();

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
    public static class Watermark {
        static final Watermark INITIAL = new Watermark(UNKNOWN_SEQ, -1);
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

        public String toString() {
            return "Watermark: " + Long.toHexString(_seqNum) + ", " + Long.toHexString(_logOffset);
        }
    }

    /**
     * Low watermark tracks the last successfully committed paxos instance (seqNum and position in log).
     * This is used as a means to filter out repeats of earlier instances and helps identify times where recovery is
     * needed. E.g. Our low watermark is 3 but the next instance we see is 5 indicating that instance 4 has likely
     * occurred without us present and we should take recovery action.
     */
	private Watermark _lowSeqNumWatermark = Watermark.INITIAL;

	private Collect _lastCollect = Collect.INITIAL;
	private long _lastLeaderActionTime = 0;

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

    public AcceptorLearner(LogStorage aStore, FailureDetector anFD, Transport aTransport) {
        this(aStore, anFD, aTransport, DEFAULT_LEASE);
    }

    public AcceptorLearner(LogStorage aStore, FailureDetector anFD, Transport aTransport, long aLeaderLease) {
        _storage = aStore;
        _transport = aTransport;
        _fd = anFD;
        _leaderLease = aLeaderLease;
    }

    public CheckpointHandle newCheckpoint() {
        // Can't allow watermark and last collect to vary independently
        //
        synchronized (this) {
            return new ALCheckpointHandle(getLowWatermark(), getLastCollect(), this);
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

	public void open() throws Exception {
		open(CheckpointHandle.NO_CHECKPOINT);
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
            synchronized(this) {
                _recoveryWindow = new RecoveryWindow(-1, Long.MAX_VALUE);
            }

            if (aHandle.equals(CheckpointHandle.NO_CHECKPOINT)) {
                // Restore logs from the beginning
                //
                _logger.info("No checkpoint - replay from the beginning");

                try {
                    new LogRangeProducer(-1, Long.MAX_VALUE, new LocalStreamer()).produce(0);
                } catch (Exception anE) {
                    _logger.error("Failed to replay log", anE);
                }

            } else if (aHandle instanceof ALCheckpointHandle) {
                ALCheckpointHandle myHandle = (ALCheckpointHandle) aHandle;

                try {
                    new LogRangeProducer(installCheckpoint(myHandle), Long.MAX_VALUE, new LocalStreamer()).produce(0);
                } catch (Exception anE) {
                    _logger.error("Failed to replay log", anE);
                }

            } else
                throw new IllegalArgumentException("Not a valid CheckpointHandle: " + aHandle);
        } finally {
            synchronized (this) {
                _recoveryWindow = null;
            }
        }
    }

    private class LocalStreamer implements Consumer {

        public void process(PaxosMessage aMsg) {
            AcceptorLearner.this.process(aMsg);
        }
    }

    public void close() {
        try {
            /*
             * Allow for the fact we might be actively processing packets - Mark ourselves out of date so we
             * cease processing, then clean up.
             */
            synchronized(this) {
                _outOfDate = new OutOfDate(_transport.getLocalAddress());
            }

            try {
                Thread.sleep(SHUTDOWN_PAUSE);
            } catch (Exception anE) {}

            synchronized(this) {
                _watchdog.cancel();
                _recoveryAlarm = null;
                _recoveryWindow = null;
                _packetBuffer.clear();
                _lastCollect = Collect.INITIAL;
                _lastLeaderActionTime = 0;
                _cachedBegins.clear();
                _lowSeqNumWatermark = Watermark.INITIAL;
            }

            _storage.close();

        } catch (Exception anE) {
            _logger.error("Failed to close logger", anE);
            throw new Error(anE);
        }
    }

    private long installCheckpoint(ALCheckpointHandle aHandle) {
        long myMin = -1;

        _lastCheckpoint = aHandle;
        _lowSeqNumWatermark = aHandle.getLowWatermark();
        _lastCollect = aHandle.getLastCollect();

        _logger.info("Checkpoint installed: " + _lastCollect + " @ " + _lowSeqNumWatermark);

        if (!_lowSeqNumWatermark.equals(Watermark.INITIAL)) {
            myMin = _lowSeqNumWatermark.getSeqNum();
        }

        return myMin;
    }

    /**
     * When an AL is out of date, call this method to bring it back into sync from a remotely sourced
     * checkpoint.
     *
     * @param aHandle obtained from the remote checkpoint.
     * @throws Exception
     */
    public void bringUpToDate(CheckpointHandle aHandle) throws Exception {
        if (! isOutOfDate())
            throw new IllegalStateException("Not out of date");

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
            _storage.mark(myHandle.getLowWatermark().getLogOffset(), true);

            _outOfDate = null;
            _recoveryWindow = null;
            _packetBuffer.clear();
            _cachedBegins.clear();

            /*
             * We do not want to allow a leader to immediately over-rule us, make it work a bit,
             * otherwise we risk leader jitter
             */
            _lastLeaderActionTime = System.currentTimeMillis();
        }
    }

    /* ********************************************************************************************
     *
     * Utility methods
     * 
     ******************************************************************************************** */

	public long getLeaderLeaseDuration() {
		return _leaderLease;
	}

    public boolean isOutOfDate() {
        synchronized(this) {
            return (_outOfDate != null);
        }
    }

	public boolean isRecovering() {
		synchronized(this) {
			return (_recoveryWindow != null);
		}
	}
	
	public void add(Paxos.Listener aListener) {
		synchronized(_listeners) {
			_listeners.add(aListener);
		}
	}

	public void remove(Paxos.Listener aListener) {
		synchronized(_listeners) {
			_listeners.remove(aListener);
		}
	}

    void signal(Event aStatus) {
        List<Paxos.Listener> myListeners;

        synchronized(_listeners) {
            myListeners = new ArrayList<Paxos.Listener>(_listeners);
        }

        for (Paxos.Listener myTarget : myListeners)
            myTarget.done(aStatus);
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
		synchronized(this) {
			if (_lowSeqNumWatermark.getSeqNum() == (aSeqNum - 1)) {
				_lowSeqNumWatermark = new Watermark(aSeqNum, aLogOffset);

				_logger.info("AL:Low :" + _lowSeqNumWatermark + ", " + _transport.getLocalAddress());
			}
		}
	}

	public Watermark getLowWatermark() {
		synchronized(this) {
			return _lowSeqNumWatermark;
		}
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
     * Leader reasoning
     *
     ******************************************************************************************** */

	public Collect getLastCollect() {
		synchronized(this) {
			return _lastCollect;
		}
	}

	/**
	 * @param aCollect
	 *            should be tested to see if it supercedes the current COLLECT
	 * @return <code>true</code> if it supercedes, <code>false</code> otherwise
	 */
	private boolean supercedes(Collect aCollect) {
		synchronized(this) {
			if (aCollect.supercedes(_lastCollect)) {
				_lastCollect = aCollect;

				return true;
			} else {
				return false;
			}
		}
	}

	private boolean originates(Begin aBegin) {
		synchronized(this) {
			return aBegin.originates(_lastCollect);
		}
	}

	private boolean precedes(Begin aBegin) {
		synchronized(this) {
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
		synchronized(this) {
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
		synchronized(this) {
			return aCollect.sameLeader(_lastCollect);
		}
	}

	private void updateLastActionTime(long aTime) {
		_logger.debug("AL:Updating last action time: " + aTime + ", " + _transport.getLocalAddress());

		synchronized(this) {
			if (aTime > _lastLeaderActionTime)
				_lastLeaderActionTime = aTime;
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

        // So far out of date, we need cleaning up and reatarting with new state so wait for that to happen.
        //
        if (isOutOfDate())
            return;

		if (isRecovering()) {
			// If the packet is a recovery request, ignore it
			//
			if (aMessage.getType() == Operations.NEED)
				return;

            // If the packet is out of date, we need a state restore
            //
            if (aMessage.getType() == Operations.OUTOFDATE) {
                synchronized(this) {
                    _outOfDate = (OutOfDate) aMessage;
                    completedRecovery();
                }

                signal(new Event(Event.Reason.OUT_OF_DATE, mySeqNum, new ConsolidatedValue()));
                return;
            }

			// If the packet is for a sequence number above the recovery window - save it for later
			//
			if (mySeqNum > _recoveryWindow.getMaxSeqNum()) {
				synchronized(this) {
					_packetBuffer.add(aMessage);
				}
				
			// If the packet is for a sequence number within the window, process it now for catchup
			//
			} else if (mySeqNum > _recoveryWindow.getMinSeqNum()) {
				process(aMessage);
				
				/*
				 *  If the low watermark is now at the top of the recovery window, we're ready to resume once we've
				 *  streamed through the packet buffer
				 */
				if (getLowWatermark().getSeqNum() == _recoveryWindow.getMaxSeqNum()) {
					synchronized(this) {
						Iterator<PaxosMessage> myPackets = _packetBuffer.iterator();
						
						while (myPackets.hasNext()) {
							PaxosMessage myMessage = myPackets.next();
							
							/*
							 * Have we discovered another message that will cause recovery? If so stop early
							 * and wait for a packet to re-trigger.  
							 */
							if (myMessage.getSeqNum() > getLowWatermark().getSeqNum() + 1)
								break;
							else
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
			/*
			 * If the sequence number we're seeing is for a sequence number >
			 * lwm + 1, we've missed some packets. Recovery range r is lwm < r
			 * <= x (where x = currentMessage.seqNum) so that the round we're
			 * seeing right now is complete and we need save packets only after
			 * that point.
			 */
			if (mySeqNum > getLowWatermark().getSeqNum() + 1) {
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
					
					// Boundary is up to and including previous round
					//
					RecoveryWindow myWindow = new RecoveryWindow(getLowWatermark().getSeqNum(), mySeqNum - 1);
					
					/*
					 * Ask a node to bring us up to speed. Note that this node mightn't have all the records we need.
					 * If that's the case, it won't stream at all or will only stream some part of our recovery
					 * window. As the recovery watchdog measures progress through the recovery window, a partial
					 * recovery or no recovery will be noticed and we'll ask a new random node to bring us up to speed.
					 */
					send(new Need(myWindow.getMinSeqNum(), myWindow.getMaxSeqNum(),
							_transport.getLocalAddress()), _fd.getRandomMember(_transport.getLocalAddress()));

					// Declare recovery active - which stops this AL emitting responses
					//
					_recoveryWindow = myWindow;

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
        private final Watermark _past = getLowWatermark();

        public void run() {
            if (! isRecovering()) {
                // Someone else will do cleanup
                //
                return;
            }

            /*
             * If the watermark has advanced since we were started recovery made some progress so we'll schedule a
             * future check otherwise fail.
             */
            if (! _past.equals(getLowWatermark())) {
                _logger.info("Recovery is progressing, " + _transport.getLocalAddress());

                reschedule();
            } else {
                _logger.info("Recovery is NOT progressing - terminate, " + _transport.getLocalAddress());

                terminateRecovery();
            }
        }
    }

    private void reschedule() {
        _logger.info("Rescheduling, " + _transport.getLocalAddress());

        synchronized(this) {
            _recoveryAlarm = new Watchdog();
            _watchdog.schedule(_recoveryAlarm, calculateInteractionTimeout());
        }
    }

    public void setRecoveryGracePeriod(long aPeriod) {
        synchronized(this) {
            _gracePeriod = aPeriod;
        }
    }

    private long calculateInteractionTimeout() {
        synchronized(this) {
            return _gracePeriod;
        }
    }

    private void terminateRecovery() {
        _logger.info("Recovery terminate, " + _transport.getLocalAddress());

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
        _logger.info("Recovery complete, " + _transport.getLocalAddress());

        synchronized(this) {
            if (_recoveryAlarm != null) {
                _recoveryAlarm.cancel();
                _watchdog.purge();
                _recoveryAlarm = null;
            }

            postRecovery();
        }
    }

    private void postRecovery() {
        synchronized(this) {
            _recoveryWindow = null;
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
		long myCurrentTime = System.currentTimeMillis();
		long mySeqNum = aMessage.getSeqNum();

		_logger.info("AL: got [ " + mySeqNum + " ] : " + aMessage + ", " + _transport.getLocalAddress());

		switch (aMessage.getType()) {
			case Operations.NEED : {
				Need myNeed = (Need) aMessage;
				
				// Make sure we can dispatch the recovery request
				//
				if (myNeed.getMinSeq() < _lastCheckpoint.getLowWatermark().getSeqNum())
					_transport.send(new OutOfDate(_transport.getLocalAddress()), myNeed.getNodeId());
				
				if (myNeed.getMaxSeq() <= getLowWatermark().getSeqNum()) {
					_logger.info("Running streamer: " + _transport.getLocalAddress());
					
					new RemoteStreamer(myNeed).run();
				}
				
				break;
			}
			
			case Operations.COLLECT: {
				Collect myCollect = (Collect) aMessage;

				if (!amAccepting(myCollect, myCurrentTime)) {
					_ignoredCollects.incrementAndGet();

					_logger.info("AL:Not accepting: " + myCollect + ", "
							+ getIgnoredCollectsCount() + ", " + _transport.getLocalAddress());
					return;
				}

                // Check the leader is not out of sync
                //
                if ((aMessage.getClassification() == PaxosMessage.LEADER) &&
                        (mySeqNum <= getLowWatermark().getSeqNum())) {
                    Collect myLastCollect = getLastCollect();

                    send(new OldRound(getLowWatermark().getSeqNum(), myLastCollect.getNodeId(),
                            myLastCollect.getRndNumber(), _transport.getLocalAddress()), myNodeId);
                    return;
                }

				// If the collect supercedes our previous collect save it to disk,
				// return last proposal etc
				//
				if (supercedes(myCollect)) {
					write(aMessage, true);
					send(constructLast(mySeqNum), myNodeId);

					/*
					 * If the collect comes from the current leader (has same rnd
					 * and node), we apply the multi-paxos optimisation, no need to
					 * save to disk, just respond with last proposal etc
					 */
				} else if (isFromCurrentLeader(myCollect)) {
					send(constructLast(mySeqNum), myNodeId);

				} else {
					// Another collect has already arrived with a higher priority,
					// tell the proposer it has competition
					//
					Collect myLastCollect = getLastCollect();

					send(new OldRound(mySeqNum, myLastCollect.getNodeId(),
							myLastCollect.getRndNumber(), _transport.getLocalAddress()), myNodeId);
				}
				
				break;
			}

			case Operations.BEGIN: {
				Begin myBegin = (Begin) aMessage;

				// If the begin matches the last round of a collect we're fine
				//
				if (originates(myBegin)) {
					updateLastActionTime(myCurrentTime);
                    cacheBegin(myBegin);
                    
					write(aMessage, true);

					send(new Accept(mySeqNum, getLastCollect().getRndNumber(), 
							_transport.getLocalAddress()), myNodeId);
				} else if (precedes(myBegin)) {
					// New collect was received since the collect for this begin,
					// tell the proposer it's got competition
					//
					Collect myLastCollect = getLastCollect();

					send(new OldRound(mySeqNum, myLastCollect.getNodeId(),
							myLastCollect.getRndNumber(), _transport.getLocalAddress()), myNodeId);
				} else {
					// Quiet, didn't see the collect, leader hasn't accounted for
					// our values, it hasn't seen our last and we're likely out of sync with the majority
					//
					_logger.info("AL:Missed collect, going silent: " + mySeqNum
							+ " [ " + myBegin.getRndNumber() + " ], " + _transport.getLocalAddress());
				}
				
				break;
			}

			case Operations.SUCCESS: {
				Success mySuccess = (Success) aMessage;

				updateLastActionTime(myCurrentTime);

				if (mySeqNum <= getLowWatermark().getSeqNum()) {
					_logger.info("AL:Discarded known value: " + mySeqNum + ", " + _transport.getLocalAddress());
				} else {
                    Begin myBegin = expungeBegin(mySeqNum);

                    if ((myBegin == null) || (myBegin.getRndNumber() != mySuccess.getRndNum())) {
                        // We never saw the appropriate begin
                        //
                        _logger.info("AL: Discarding success: " + myBegin + ", " + mySuccess +
                                ", " + _transport.getLocalAddress());
                    } else {
                        // Always record the success even if it's the heartbeat so there are
                        // no gaps in the Paxos sequence
                        //
                        long myLogOffset = write(aMessage, true);

                        updateLowWatermark(mySeqNum, myLogOffset);

                        if (myBegin.getConsolidatedValue().equals(HEARTBEAT)) {
                            _receivedHeartbeats.incrementAndGet();

                            _logger.info("AL: discarded heartbeat: "
                                    + System.currentTimeMillis() + ", "
                                    + getHeartbeatCount() + ", " + _transport.getLocalAddress());
                        } else {
                            _logger.info("AL:Learnt value: " + mySeqNum + ", " + _transport.getLocalAddress());

                            signal(new Event(Event.Reason.DECISION, mySeqNum,
                                    myBegin.getConsolidatedValue()));
                        }
                    }
				}

				break;
			}

			default:
				throw new RuntimeException("Unexpected message" + ", " + _transport.getLocalAddress());
		}
	}

	private Last constructLast(long aSeqNum) {
		Watermark myLow = getLowWatermark();
		
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
			_logger.error("Failed to replay log" + ", " + _transport.getLocalAddress(), anE);
			throw new RuntimeException("Failed to replay log" + ", " + _transport.getLocalAddress(), anE);
		}
		
		if (myState.getLastValue() == null) {
			return new Last(aSeqNum, myLow.getSeqNum(), Long.MIN_VALUE,
					LogStorage.NO_VALUE, _transport.getLocalAddress());
		} else {			
			Begin myBegin = myState.getLastValue();
			
			return new Last(aSeqNum, myLow.getSeqNum(), myBegin.getRndNumber(), myBegin.getConsolidatedValue(),
					_transport.getLocalAddress());
		}
	}

    private void send(PaxosMessage aMessage, InetSocketAddress aNodeId) {
        // Stay silent during recovery to avoid producing out-of-date responses
        //
        if (! isRecovering())
            _transport.send(aMessage, aNodeId);
    }

    private static void dump(String aMessage, byte[] aBuffer) {
    	System.err.print(aMessage + " ");
    	
        for (int i = 0; i < aBuffer.length; i++) {
            System.err.print(Integer.toHexString(aBuffer[i]) + " ");
        }

        System.err.println();
    }	

    /* ********************************************************************************************
     *
     * Operation log handling
     *
     ******************************************************************************************** */

    private long write(PaxosMessage aMessage, boolean aForceRequired) {
        try {
            return getStorage().put(Codecs.encode(aMessage), aForceRequired);
        } catch (Exception anE) {
            _logger.error("AL: cannot log: " + System.currentTimeMillis() + ", " + _transport.getLocalAddress(), anE);
            throw new RuntimeException(anE);
        }
    }

    private static class Instance {
        private Begin _lastBegin;

        void add(PaxosMessage aMessage) {
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
                    } else if (myBegin.getRndNumber() > _lastBegin.getRndNumber() ){
                        _lastBegin = myBegin;
                    }

                    break;
                }

                case Operations.SUCCESS : {
                    // Nothing to do
                    //
                    break;
                }

                default : throw new RuntimeException("Unexpected message: " + aMessage);
            }
        }

        Begin getLastValue() {
            return _lastBegin;
        }

        public String toString() {
            return "LoggedInstance: " + _lastBegin.getSeqNum() + " " + _lastBegin;
        }
    }

    public interface Consumer {
        public void process(PaxosMessage aMsg);
    }

    public interface Producer {
        public void produce(long aLogOffset) throws Exception;
    }

    private class LogRangeProducer implements RecordListener, Producer {
        private long _lowerBoundSeq;
        private long _maximumSeq;
        private Consumer _consumer;

        LogRangeProducer(long aLowerBoundSeq, long aMaximumSeq, Consumer aConsumer) {
            _lowerBoundSeq = aLowerBoundSeq;
            _maximumSeq = aMaximumSeq;
            _consumer = aConsumer;
        }

        public void produce(long aLogOffset) throws Exception {
            _storage.replay(this, 0);
        }

        public void onRecord(long anOffset, byte[] aRecord) {
            PaxosMessage myMessage = Codecs.decode(aRecord);

            // Only send messages in the recovery window
            //
            if ((myMessage.getSeqNum() > _lowerBoundSeq)
                    && (myMessage.getSeqNum() <= _maximumSeq)) {
                _logger.debug("Producing: " + myMessage);
                _consumer.process(myMessage);
            } else {
                _logger.debug("Not producing: " + myMessage);
            }
        }
    }

    /**
     * Used to locate the recorded state of a specified instance of Paxos.
     */
    private class StateFinder implements Consumer {
        private Instance _state = new Instance();

        StateFinder(long aSeqNum, long aLogOffset) throws Exception {
            new LogRangeProducer(aSeqNum - 1, aSeqNum, this).produce(aLogOffset);
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
            _logger.info("RemoteStreamer starting, " + _transport.getLocalAddress());

            _stream = _transport.connectTo(_need.getNodeId());

            // Check we got a connection
            //
            if (_stream == null) {
                _logger.warn("RemoteStreamer couldn't connect: " + _need.getNodeId());
                return;
            }

            try {
                new LogRangeProducer(_need.getMinSeq(), _need.getMaxSeq(), this).produce(0);
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
