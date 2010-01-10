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
 * Implements the Acceptor/Learner state machine.  Note that the instance running in the same JVM as the current leader
 * is to all intents and purposes (bar very strange hardware or operating system failures) guaranteed to receive packets
 * from the leader.  Thus if a leader declares SUCCESS then the local instance will receive those packets.  This can be
 * useful for processing client requests correctly and signalling interested parties as necessary.
 *
 * @todo Success messages must contain the round number so we can validate they follow on from the current BEGIN.
 * If it doesn't follow on we must OLD_ROUND. In the original algorithmic description from "Paxos Made Simple" accepts
 * are broadcast to all learners so they can see a quorum for a particular round. As we use a distinguished learner,
 * in the form of the Leader, it must broadcast the quorum itself which means it must include the round number in case
 * some other leader gains acceptance and now wants to send out a value. We could fix this by sending the value out
 * during the accept phase and then have the success message be merely a commit of that value. Then a round number
 * is not required for success as any leader that gets as far as success must have accounted for the values sent in
 * accept when processing last messages. Success would just then contain the sequence number and any AL that hasn't
 * seen the associated begin would take no action (no sync of value to disk and no ack of the success), such an AL
 * would then perform recovery as the result of not incrementing it's low watermark (in response to success) and 
 * detecting a missed round at the next begin/accept.
 * 
 * @todo Checkpoint must include low watermark etc.
 * 
 * @author dan
 */
public class AcceptorLearner {
    public static final ConsolidatedValue HEARTBEAT = 
    	new ConsolidatedValue("org.dancres.paxos.Heartbeat".getBytes(), new byte[]{});

    private static long DEFAULT_LEASE = 30 * 1000;
    private static Logger _logger = LoggerFactory.getLogger(AcceptorLearner.class);

    /**
     * Statistic that tracks the number of Collects this AcceptorLearner ignored from competing leaders within
     * DEFAULT_LEASE ms of activity from the current leader.
     */
    private AtomicLong _ignoredCollects = new AtomicLong();
    private AtomicLong _receivedHeartbeats = new AtomicLong();

    /**
     * @todo Must checkpoint _lastCollect, as it'll only be written in the log file the first time it appears and thus
     * when we hit a checkpoint it will be discarded. This is because we implement the optimisation for multi-paxos
     * described in "Paxos Made Simple" such that we needn't sync to disk for a Collect that is identical to and comes
     * from the same Leader as for previous rounds.
     */
    private Collect _lastCollect = Collect.INITIAL;
    private long _lastLeaderActionTime = 0;

    private LogStorage _storage;

    /**
     * Tracks the last contiguous sequence number for which we have a value. A value of -1 indicates we have yet
     * to see a proposals.
     * 
     * When we receive a success, if it's seqNum is this field + 1, increment this field.  Acts as the low watermark for
     * leader recovery, essentially we want to recover from the last contiguous sequence number in the stream of paxos
     * instances.
     */
    private long _lowSeqNumWatermark = LogStorage.NO_SEQ;

    /**
     * PacketBuffer is used to maintain a limited amount of past Paxos history that can be used to catch-up
     * recovering nodes without the cost of a full transfer of logs etc. This is useful in cases where nodes
     * temporarily fail due to loss of a network connection or reboots.
     */
    private PacketBuffer _buffer = new PacketBuffer(512);
    
    private final List<AcceptorLearnerListener> _listeners = new ArrayList<AcceptorLearnerListener>();

    /*
     * Introducing recovery:
     * 
     * Low watermark is -1
     * When packet arrives, unless it's for seqnum 0 (which indicates this is the initial paxos execution) we'll store
     * it in packet buffer and trigger recovery (which should set a flag which causes further recovery triggers to
     * be skipped but stores packets up meantime).
     * 
     * Recovery looks at current low watermark, sees it's -1 and opens log, restoring checkpointed state and then
     * playing through any outstanding packets in the log. On completion, it dispatches a RESTORED packet
     * into AcceptorLearner.
     * 
     * AcceptorLearner sees RESTORED packet and proceeds to try and replay any packets out of the packet buffer, if it
     * can't it triggers another recovery. (The packets replayed will be those "since" the last packet recovered from
     * the log of the other node).
     *  
     * Recovery looks at current low watermark, sees it's not -1 and determines the log_id of the last log record 
     * committed by this AL. It then requests the log from another node from this log_id onwards and plays those
     * packets through the AL to catch it up. If it succeeds, it dispatches a RESTORED packet
     * to AcceptorLearner and behaviour is as above. 
     * 
     * If it fails (because the log_id is no longer in the valid range of the target's log, it closes the log 
     * (which will have been opened in the initial recovery case at least), delete's the files and attempts to grab
     * checkpoint and log from a node which has state up to and including at least low watermark seq num + 1.
     * 
     * Impl Ideas:
     * 
     * Perhaps when AL starts up (and when it triggers further recoveries beyond initial startup recovery) it has
     * an instance of a recoverer stored in a member. As the member is non-null, it sends all received packets to it
     * which can then be restored by it when it's completed its other recovery tasks. Also, because the member is
     * non-null it doesn't attempt further recoveries. The fact the field is set also indicates to the Leader what's
     * going on. To make this work means separating the public entry point for packet reception from the inner
     * processing layer. The public entry point does a check for the recoverer and then submits it to the inner
     * layer. The inner layer is directly accessed by the recovery routines so they can drive through packets to effect
     * state transitions.
     * 
     * Maybe the log doesn't consist of packets, just state transitions which could be re-applied to AL state. Packets
     * are only stored temporarily during recovery and when asked for a LAST we consult the log via a replay for the
     * relevant SUCCESS transition. When packets are fed through, if they are out of date according to the low watermark
     * we simply ignore them. The advantage of state transition is we don't need to re-parse the packet, skipping
     * around recovery and such. 
     * 
     * The low watermark should consist of two elements, the sequence number and the associated log_id. We'd use the
     * sequence number to rule out old packets and the log number to help us during recovery when we need to ask for
     * logs from another node.
     * 
     * We don't need the high watermark as we don't store a value during a BEGIN only during a SUCCESS. This means
     * we can compute the range to recover purely from all the low watermarks (which are updated every time we see
     * a contiguous SUCCESS). If the low watermark is lower than any leader's seen so far, it's the minimum. If it's
     * greater than any we've seen before, it's the maximum.
     * 
     * @todo:
 	 * A leader when consulting it's AL may then issue a collect for something that is way out of date
     * and thus would need to recover itself. How do we signal that back via a LAST and how does it do recovery?
     */
    public AcceptorLearner(LogStorage aStore) {
        _storage = aStore;
        
        try {
        	_storage.open();
        } catch (Exception anE) {
        	_logger.error("Failed to open logger", anE);
        	throw new RuntimeException(anE);
        }
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

    private void updateLowWatermark(long aSeqNum) {
        synchronized(this) {
            if (_lowSeqNumWatermark == (aSeqNum - 1)) {
                _lowSeqNumWatermark = aSeqNum;

                _logger.info("Low watermark:" + aSeqNum);
            }

        }
    }

    public long getLowWatermark() {
        synchronized(this) {
            return _lowSeqNumWatermark;
        }
    }

    /**
     * @param aCollect should be tested to see if it supercedes the current COLLECT
     * @return <code>true</code> if it supercedes, <code>false</code> otherwise
     */
    private boolean supercedes(Collect aCollect) {
        synchronized(this) {
            if (aCollect.supercedes(_lastCollect)) {
                Collect myOld = _lastCollect;
                _lastCollect = aCollect;

                return true;
            } else {
                return false;
            }
        }
    }

    public Collect getLastCollect() {
        synchronized(this) {
            return _lastCollect;
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
     * @return <code>true</code> if the collect is either from the existing leader, or there is no leader or there's
     * been nothing heard from the current leader within DEFAULT_LEASE milliseconds else <code>false</code>
     */
    private boolean amAccepting(Collect aCollect, long aCurrentTime) {
        synchronized(this) {
            if (_lastCollect.isInitial()) {
                return true;
            } else {
                if (isFromCurrentLeader(aCollect))
                    return true;
                else
                    return (aCurrentTime > _lastLeaderActionTime + DEFAULT_LEASE);
            }
        }
    }

    private boolean isFromCurrentLeader(Collect aCollect) {
    	synchronized(this) {
    		return aCollect.sameLeader(_lastCollect);
    	}
    }
    
    private void updateLastActionTime(long aTime) {
        _logger.info("Updating last action time: " + aTime);

        synchronized(this) {
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

        _logger.info("AcceptorLearnerState got [ " + mySeqNum + " ] : " + aMessage);

        switch (aMessage.getType()) {
            case Operations.COLLECT : {
                Collect myCollect = (Collect) aMessage;

                if (! amAccepting(myCollect, myCurrentTime)) {
                    _ignoredCollects.incrementAndGet();

                    _logger.info("Not accepting: " + myCollect + ", " + getIgnoredCollectsCount());                 
                    return null;
                }

                // If the collect supercedes our previous collect sace it to disk, return last proposal etc
                //
                if (supercedes(myCollect)) {
                    write(aMessage, true);
                    
                    /*
                     *  @TODO FIX THIS!!!!
                     *  
                     *  Rnd number should be the round of the last proposal we saw for this sequence number or 
                     *  some form of null value. Similarly the value should either come from the highest numbered
                     *  proposal we've seen or the log/packet buffer.
                     */
                    return new Last(mySeqNum, getLowWatermark(), Long.MIN_VALUE, LogStorage.NO_VALUE);
                
                /*
                 *  If the collect comes from the current leader (has same rnd and node), we apply the multi-paxos
                 *  optimisation, no need to save to disk, just respond with last proposal etc
                 */
                } else if (isFromCurrentLeader(myCollect)) {                	
                    /*
                     *  @TODO FIX THIS!!!!
                     *  
                     *  Rnd number should be the round of the last proposal we saw for this sequence number or 
                     *  some form of null value. Similarly the value should either come from the highest numbered
                     *  proposal we've seen or the log/packet buffer.
                     */
                    return new Last(mySeqNum, getLowWatermark(), Long.MIN_VALUE, LogStorage.NO_VALUE);
                    
                } else {
                    // Another collect has already arrived with a higher priority, tell the proposer it has competition
                    //
                    Collect myLastCollect = getLastCollect();

                    return new OldRound(mySeqNum, myLastCollect.getNodeId(), myLastCollect.getRndNumber());
                }
            }

            case Operations.BEGIN : {
                Begin myBegin = (Begin) aMessage;

                // If the begin matches the last round of a collect we're fine
                //
                if (originates(myBegin)) {
                    updateLastActionTime(myCurrentTime);
                    write(aMessage,true);
                    
                    return new Accept(mySeqNum, getLastCollect().getRndNumber());
                } else if (precedes(myBegin)) {
                    // New collect was received since the collect for this begin, tell the proposer it's got competition
                    //
                    Collect myLastCollect = getLastCollect();

                    return new OldRound(mySeqNum, myLastCollect.getNodeId(), myLastCollect.getRndNumber());
                } else {
                    // Quiet, didn't see the collect, leader hasn't accounted for our values, it hasn't seen our last
                    //
                    _logger.info("Missed collect, going silent: " + mySeqNum + " [ " + myBegin.getRndNumber() + " ]");
                }
            }
            
            case Operations.SUCCESS : {
                Success mySuccess = (Success) aMessage;

                _logger.info("Learnt value: " + mySuccess.getSeqNum());

                updateLastActionTime(myCurrentTime);
                updateLowWatermark(mySuccess.getSeqNum());

                // Always record the value even if it's the heartbeat so there are no gaps in the Paxos sequence
                //
                write(aMessage, true);
                
                if (mySuccess.getConsolidatedValue().equals(HEARTBEAT)) {
                    _receivedHeartbeats.incrementAndGet();

                    _logger.info("AcceptorLearner discarded heartbeat: " + System.currentTimeMillis() + ", " +
                            getHeartbeatCount());
                } else {
                	signal(new Event(Event.Reason.DECISION, 
                			mySuccess.getSeqNum(), mySuccess.getConsolidatedValue(), null));
                }

                return new Ack(mySuccess.getSeqNum());
            }

            default : throw new RuntimeException("Unexpected message");
        }
    }

    private void write(PaxosMessage aMessage, boolean aForceRequired) {
        try {
        	synchronized(this) {
        		_buffer.add(aMessage, getStorage().put(Codecs.encode(aMessage), aForceRequired));
        	}
        } catch (Exception anE) {
        	_logger.error("Acceptor cannot log: " + System.currentTimeMillis(), anE);
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
}
