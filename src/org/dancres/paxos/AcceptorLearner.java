package org.dancres.paxos;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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
 * @todo Implement recovery from any node. With the timeout mechanism in place it's possible to retry multiple nodes
 * rather than rely on leader. When recovery first triggers, select a node from membership at random and send it a
 * need request. If we timeout select a node and try again.
 *
 * @author dan
 */
public class AcceptorLearner {
    private static final long DEFAULT_GRACE_PERIOD = 30 * 1000;

    public static final long UNKNOWN_SEQ = -1;
    
	public static final ConsolidatedValue HEARTBEAT = new ConsolidatedValue(
			"org.dancres.paxos.Heartbeat".getBytes(), new byte[] {});

    public static final ConsolidatedValue OUT_OF_DATE = new ConsolidatedValue(
            "org.dancres.paxos.OOD".getBytes(), new byte[] {});

	private static long DEFAULT_LEASE = 30 * 1000;
	private static Logger _logger = LoggerFactory
			.getLogger(AcceptorLearner.class);

    private final Timer _watchdog = new Timer("AcceptorLearner timers");
    private TimerTask _recoveryAlarm = null;

	/**
	 * Statistic that tracks the number of Collects this AcceptorLearner ignored
	 * from competing leaders within DEFAULT_LEASE ms of activity from the
	 * current leader.
	 */
	private AtomicLong _ignoredCollects = new AtomicLong();
	private AtomicLong _receivedHeartbeats = new AtomicLong();

    private long _gracePeriod = DEFAULT_GRACE_PERIOD;

    private OutOfDate _outOfDate = null;
	private RecoveryWindow _recoveryWindow = null;
	private List<PaxosMessage> _packetBuffer = new LinkedList<PaxosMessage>();
	
	/**
     * Tracks the last collect we accepted and thus represents our view of the current leader for instances.
     * Other leaders wishing to take over must present a round number greater than this and after expiry of the
     * lease for the previous leader. The last collect is also used to validate begins and ensure we don't accept
     * any from other leaders whilst we're not in sync with the majority.
     *
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
     * Begins contain the values being proposed. These values must be remembered from round to round of an instance
     * until it has been committed. For any instance we believe is unresolved we keep the last value proposed (from
     * the last acceptable Begin) cached (the begins are also logged to disk) such that when we see Success for an
     * instance we remove the value from the cache and send it to any listeners. This saves us having to disk scans
     * for values and also placing the value in the Success message.
     */
    private final Map<Long, Begin> _cachedBegins = new HashMap<Long, Begin>();

    /**
     * Low watermark tracks the last successfully committed paxos instance (seqNum and position in log).
     * This is used as a means to filter out repeats of earlier instances and helps identify times where recovery is
     * needed. E.g. Our low watermark is 3 but the next instance we see is 5 indicating that instance 4 has likely
     * occurred without us present and we should take recovery action.
     */
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
	
	public void add(AcceptorLearnerListener aListener) {
		synchronized(_listeners) {
			_listeners.add(aListener);
		}
	}

	public void remove(AcceptorLearnerListener aListener) {
		synchronized(_listeners) {
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
		synchronized(this) {
			if (_lowSeqNumWatermark.getSeqNum() == (aSeqNum - 1)) {
				_lowSeqNumWatermark = new Watermark(aSeqNum, aLogOffset);

				_logger.info("AL:Low :" + _lowSeqNumWatermark);
			}
		}
	}

	public Watermark getLowWatermark() {
		synchronized(this) {
			return _lowSeqNumWatermark;
		}
	}

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
		_logger.info("AL:Updating last action time: " + aTime);

		synchronized(this) {
			if (aTime > _lastLeaderActionTime)
				_lastLeaderActionTime = aTime;
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

                signal(new Event(Event.Reason.OUT_OF_DATE, mySeqNum, OUT_OF_DATE, null));
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
				 * We need to catchup to the end of a complete paxos instance so we must wait for a COLLECT that is
				 * > lwm + 1 which will thus contain a sequence number to bound the recovery.  
				 */
				if (aMessage.getType() != Operations.COLLECT)
					return;
				
				synchronized(this) {
					// Must store up the packet which triggered recovery for later replay
					//
					_packetBuffer.add(aMessage);
					
					// Boundary is a collect which starts next round - we need up to and including previous round
					//
					RecoveryWindow myWindow = new RecoveryWindow(getLowWatermark().getSeqNum(), mySeqNum - 1);
					
					// Ask the current leader to bring us back up to speed
					//
					send(new Need(myWindow.getMinSeqNum(), myWindow.getMaxSeqNum(), 
							_transport.getLocalAddress()), aMessage.getNodeId());

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
                _logger.info("Recovery is progressing");

                reschedule();
            } else {
                _logger.info("Recovery is NOT progressing - terminate");

                terminateRecovery();
            }
        }
    }

    private void reschedule() {
        _logger.info("Rescheduling");

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
        _logger.info("Recovery terminate");

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
        _logger.info("Recovery complete");

        synchronized(this) {
            if (_recoveryAlarm != null) {
                _recoveryAlarm.cancel();
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

	/**
	 * @todo Implement too-out-of-date response which will be picked up by the requesting AL and cause it to
	 * do a full by-file resync or inform client too far out of date and to get another checkpoint etc.
	 *
	 * @param aMessage is the message to process
	 * @return is any message to send
	 */
	private void process(PaxosMessage aMessage) {
		InetSocketAddress myNodeId = aMessage.getNodeId();
		long myCurrentTime = System.currentTimeMillis();
		long mySeqNum = aMessage.getSeqNum();

		_logger.info("AL: got [ " + mySeqNum + " ] : " + aMessage);

		switch (aMessage.getType()) {
			case Operations.NEED : {
				Need myNeed = (Need) aMessage;
				
				// Make sure we can dispatch the recovery request
				//
				if (myNeed.getMaxSeq() <= getLowWatermark().getSeqNum()) {
					_logger.info("Running streamer");
					
					new Streamer(myNeed).run();
				}
				
				break;
			}
			
			case Operations.COLLECT: {
				Collect myCollect = (Collect) aMessage;

				if (!amAccepting(myCollect, myCurrentTime)) {
					_ignoredCollects.incrementAndGet();

					_logger.info("AL:Not accepting: " + myCollect + ", "
							+ getIgnoredCollectsCount());
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
							+ " [ " + myBegin.getRndNumber() + " ]");
				}
				
				break;
			}

			case Operations.SUCCESS: {
				Success mySuccess = (Success) aMessage;

				updateLastActionTime(myCurrentTime);

				if (mySeqNum <= getLowWatermark().getSeqNum()) {
					_logger.info("AL:Discarded known value: " + mySeqNum);
				} else {
                    Begin myBegin = expungeBegin(mySeqNum);

                    if ((myBegin == null) || (myBegin.getRndNumber() != mySuccess.getRndNum())) {
                        // We never saw the appropriate begin
                        //
                        _logger.info("AL: Discarding success: " + myBegin + ", " + mySuccess);
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
                                    + getHeartbeatCount());
                        } else {
                            _logger.info("AL:Learnt value: " + mySeqNum);

                            signal(new Event(Event.Reason.DECISION, mySeqNum,
                                    myBegin.getConsolidatedValue(), null));
                        }
                    }
				}

				break;
			}

			default:
				throw new RuntimeException("Unexpected message");
		}
	}

	private void send(PaxosMessage aMessage, InetSocketAddress aNodeId) {
        // Stay silent during recovery to avoid producing out-of-date responses
        //
		if (! isRecovering())
			_transport.send(aMessage, aNodeId);
	}
	
	private Last constructLast(long aSeqNum) {
		Watermark myLow = getLowWatermark();
		
		/*
		 * If the sequence number is less than the current low watermark, we've got to check through the log file for
		 * the value otherwise if it's present, it will be since the low watermark offset.
		 */
		InstanceState myState;
		
		try {
			if ((myLow.equals(Watermark.INITIAL)) || (aSeqNum <= myLow.getSeqNum())) {
				myState = new ReplayListenerImpl(aSeqNum, 0).getState();
			} else 
				myState = new ReplayListenerImpl(aSeqNum, myLow.getLogOffset()).getState();
		} catch (Exception anE) {
			_logger.error("Failed to replay log", anE);
			throw new RuntimeException("Failed to replay log", anE);
		}
		
		if ((myState == null) || (myState.getLastValue() == null)) {
			return new Last(aSeqNum, myLow.getSeqNum(), Long.MIN_VALUE,
					LogStorage.NO_VALUE, _transport.getLocalAddress());
		} else {			
			Begin myBegin = myState.getLastValue();
			
			return new Last(aSeqNum, myLow.getSeqNum(), myBegin.getRndNumber(), myBegin.getConsolidatedValue(),
					_transport.getLocalAddress());
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

	void signal(Event aStatus) {
		List<AcceptorLearnerListener> myListeners;

		synchronized(_listeners) {
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

        Begin getLastValue() {
            return _lastBegin;
        }

        public String toString() {
            return "LoggedInstance: " + _seqNum + " " + _lastBegin + " @ " + Long.toHexString(_lastBeginOffset) +
                " " + _lastSuccess + " @ " + Long.toHexString(_lastSuccessOffset);
        }
    }

    private class ReplayListenerImpl implements RecordListener {
        private long _seqNum;
        private InstanceState _state = null;

        ReplayListenerImpl(long aSeqNum, long aLogOffset) throws Exception {
            _seqNum = aSeqNum;
            _storage.replay(this, aLogOffset);
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

    private class Streamer extends Thread implements RecordListener {
        private Need _need;
        private Stream _stream;

        Streamer(Need aNeed) {
            _need = aNeed;
        }

        public void run() {
            _logger.info("Streamer starting");

            _stream = _transport.connectTo(_need.getNodeId());

            // Check we got a connection
            //
            if (_stream == null) {
                _logger.warn("Stream couldn't connect: " + _need.getNodeId());
                return;
            }

            try {
                _storage.replay(this, 0);
            } catch (Exception anE) {
                _logger.error("Failed to replay log", anE);
            }

            _stream.close();
        }

        public void onRecord(long anOffset, byte[] aRecord) {
            PaxosMessage myMessage = Codecs.decode(aRecord);

            // Only send messages in the recovery window
            //
            if ((myMessage.getSeqNum() > _need.getMinSeq())
                    && (myMessage.getSeqNum() <= _need.getMaxSeq())) {
                _logger.debug("Streaming: " + myMessage);
                _stream.send(myMessage);
            } else {
                _logger.debug("Not streaming: " + myMessage);
            }
        }
    }

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
}
