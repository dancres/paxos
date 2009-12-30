package org.dancres.paxos;

import org.dancres.paxos.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implements the leader state machine.
 *
 * @todo State recovery - basic model will be to resolve memory state from the last checkpoint and then replay the log
 * to bring that memory state up-to-date (possibly by re-announcing completions via the AcceptorLearner).  Once that
 * is done, we could become the leader.
 * @todo If some other leader failed, we need to resolve any previous slots outstanding. It's possible the old leader
 * secured a slot and declared a value but didn't report back to the client. In that case, the client would see it's
 * leader fail to respond and have to either submit an operation to determine whether it's previous attempt succeeded
 * or submit an idempotent operation that could be retried safely multiple times. Other cases include the leader
 * reserving a slot but not filling in a value in which case we need to fill it with a null value (which AcceptorLearner
 * would need to drop silently rather than deliver it to a listener). It's also possible the leader proposed a value
 * but didn't reach a majority due to network issues and then died. In such a case, the new leader will have to settle
 * that round with the partially agreed value.  It's likely that will be done with the originating client absent so
 * as per the leader failed to report case, the client will need to retry with the appropriate protocol.
 * @todo Add a test for validating multiple sequence number recovery.
 * 
 * @author dan
 */
public class Leader implements MembershipListener {
    private static final Logger _logger = LoggerFactory.getLogger(Leader.class);

    /*
     * Used to compute the timeout period for watchdog tasks.  In order to behave sanely we want the failure 
     * detector to be given the best possible chance of detecting problems with the members.  Thus the timeout for the
     * watchdog is computed as the unresponsiveness threshold of the failure detector plus a grace period.
     */
    private static final long FAILURE_DETECTOR_GRACE_PERIOD = 2000;

    /*
     * Internal states
     */

    /**
     * Leader reaches this state after SUBMITTED. If already leader, a transition to BEGIN will be immediate.
     * If not leader and recovery is not active, move to state RECOVER otherwise leader is in recovery and must now
     * settle low to high watermark (as set by RECOVER) before processing any submitted value by repeatedly executing
     * full instances of paxos (including a COLLECT to recover any previous value that was proposed).
     */
    private static final int COLLECT = 0;

    /**
     * Attempt to reserve a slot in the sequence of operations. Transition to SUCCESS after emitting begin to see
     * if the slot was granted.
     */
    private static final int BEGIN = 1;

    /**
     * Leader has sent a BEGIN and now determines if it has secured the slot associated with the sequence number.
     * If the slot was secured, a value will be sent to all members of the current instance after which there will
     * be a transition to COMMITTED.
     */
    private static final int SUCCESS = 2;

    /**
     * A paxos instance was completed successfully, clean up is all that remains.
     */
    private static final int EXIT = 3;

    /**
     * A paxos isntance failed for some reason (which will be found in </code>_completion</code>).
     */
    private static final int ABORT = 4;
    private static final int RECOVER = 5;

    /**
     * Leader has completed the necessary steps to secure an entry in storage for a particular sequence number
     * and is now processing ACKs to ensure enough nodes have seen the committed value.
     */
    private static final int COMMITTED = 6;

    /**
     * Leader has been given a value and should attempt to complete a paxos instance.
     */
    private static final int SUBMITTED = 7;

    /**
     * Indicates the Leader is not ready to process the passed message and the caller should retry.
     */
    public static final int BUSY = 256;

    /**
     * Indicates the Leader has accepted the message and is waiting for further messages.
     */
    public static final int ACCEPTED = 257;

    private final Timer _watchdog = new Timer("Leader timers");
    private final long _watchdogTimeout;
    private final FailureDetector _detector;
    private final NodeId _nodeId;
    private final Transport _transport;
    private final AcceptorLearner _al;

    private long _seqNum = LogStorage.EMPTY_LOG;

    /**
     * Tracks the round number this leader used last. This cannot be stored in the acceptor/learner's version without 
     * risking breaking the collect protocol (the al could suddenly believe it's seen a collect it hasn't and become
     * noisy when it should be silent). This field can be updated as the result of OldRound messages.
     */
    private long _rndNumber = 0;

    /**
     * The current value the leader is using for a proposal. Might be sourced from _clientOp but can also come from 
     * Last messages as part of recovery.
     */
    private byte[] _value;

    /**
     * Note that being the leader is merely an optimisation and saves on sending COLLECTs.  Thus if one thread 
     * establishes we're leader and a prior thread decides otherwise with the latter being last to update this variable
     * we'll simply do an unnecessary COLLECT.  The protocol execution will still be correct.
     */
    private boolean _isLeader = false;

    /**
     * When in recovery mode we perform collect for each sequence number in the recovery range
     */
    private boolean _isRecovery = false;

    private long _lowWatermark;
    private long _highWatermark;

    /**
     * Maintains the current client request. The actual sequence number and value the state machine operates on are
     * held in <code>_seqNum</code> and <code>_value</code> and during recovery will not be the same as the client
     * request.  Thus we cache the client request and move it into the operating variables once recovery is complete.
     */
    private Post _clientOp;

    /**
     * This alarm is used to limit the amount of time the leader will wait for responses from all apparently live
     * members in a round of communication.
     */
    private TimerTask _interactionAlarm;

    /**
     * This alarm is used to ensure the leader sends regular heartbeats in the face of inactivity so as to extend
     * its lease with AcceptorLearners.
     */
    private TimerTask _heartbeatAlarm;

    /**
     * Tracks membership for an entire paxos instance.
     */
    private Membership _membership;

    private int _stage = EXIT;
    
    /**
     * In cases of ABORT, indicates the reason
     */
    private Completion _completion;

    private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

    private List<Post> _queue = new LinkedList<Post>();

    public Leader(FailureDetector aDetector, NodeId aNodeId, Transport aTransport, AcceptorLearner anAcceptorLearner) {
        _nodeId = aNodeId;
        _detector = aDetector;
        _transport = aTransport;
        _watchdogTimeout = _detector.getUnresponsivenessThreshold() + FAILURE_DETECTOR_GRACE_PERIOD;
        _al = anAcceptorLearner;
    }

    private long calculateLeaderRefresh() {
        long myExpiry = _al.getLeaderLeaseDuration();
        return myExpiry - (myExpiry * 20 / 100);
    }

    public long getCurrentRound() {
        synchronized(this) {
            return _rndNumber;
        }
    }

    private long getRndNumber() {
        return _rndNumber;
    }

    private long newRndNumber() {
        return ++_rndNumber;
    }

    /**
     * @param anOldRound is the instance to update from
     */
    private void updateRndNumber(OldRound anOldRound) {
        updateRndNumber(anOldRound.getLastRound());
    }

    /**
     * Updates the leader's view of what the current round number is. We don't increment it here, that's done when
     * we attempt to become leader. We're only concerned here with having as up-to-date view as possible of what
     * the current view is.
     *
     * @param aNumber the round number we wish to update to
     */
    private void updateRndNumber(long aNumber) {
        _rndNumber = aNumber;
    }

    private boolean isRecovery() {
        return _isRecovery;
    }

    private void amRecovery() {
        _isRecovery = true;
    }

    private void notRecovery() {
        _isRecovery = false;
    }

    private boolean isLeader() {
        return _isLeader;
    }

    private void amLeader() {
        _isLeader = true;
    }

    private void notLeader() {
        _isLeader = false;
    }

    public NodeId getNodeId() {
        return _nodeId;
    }

    public boolean isReady() {
        synchronized(this) {
            return (_stage == EXIT) || (_stage == ABORT);
        }
    }

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we
     * process the result of a Collect in the BEGIN state which means we expect Last or OldRound and in SUCCESS state
     * we expect ACCEPT or OLDROUND
     */
    private void process() {
        switch(_stage) {
            case ABORT : {
                _logger.info("Leader::ABORT " + _completion);

                if (_membership != null)
                    _membership.dispose();

                _al.signal(_completion);

                /*
                 * If there are any queued operations we must fail those also - use the same completion code...
                 */
                while (_queue.size() > 0) {
                    Post myOp = (Post) _queue.remove(0);
                    _al.signal(new Completion(_completion.getResult(), _completion.getSeqNum(),
                            myOp.getConsolidatedValue()));
                }

                return;
            }

            case EXIT : {
                _logger.info("Leader::EXIT " + _completion);

                if (_membership != null)
                    _membership.dispose();

                if (isRecovery()) {
                    /*
                     * We've completed a paxos instance as part of recovery, we must return to COLLECT via SUBMITTED
                     * (which sets up membership etc) to continue recovery of process the original request that
                     * triggered us to assume leadership and recover.
                     */
                    _stage = SUBMITTED;
                    process();

                } else if (_queue.size() > 0) {
                    _clientOp = _queue.remove(0);

                    _logger.info("Processing op from queue: " + _clientOp);

                    _stage = SUBMITTED;
                    process();

                } else {
                    _heartbeatAlarm = new HeartbeatTask();
                    _watchdog.schedule(_heartbeatAlarm, calculateLeaderRefresh());
                }

                return;
            }

            case SUBMITTED : {
                _membership = _detector.getMembers(this);

                _logger.info("Got membership for leader: " + Long.toHexString(_seqNum) + ", (" +
                        _membership.getSize() + ")");

                // Collect will decide if it can skip straight to a begin
                //
                _stage = COLLECT;
                process();

                break;
            }

            case COLLECT : {
                if ((! isLeader()) && (! isRecovery())) {
                    _logger.info("Not currently leader, recovering");

                    /*
                     * If the Acceptor/Learner thinks there's a valid leader and the failure detector confirms
                     * liveness, reject the request
                     */
                    Collect myLastCollect = _al.getLastCollect();

                    // Collect is INITIAL means no leader known so try to become leader
                    //
                    if (! myLastCollect.isInitial()) {
                      NodeId myOtherLeader = NodeId.from(myLastCollect.getNodeId());

                      if (_detector.isLive(myOtherLeader)) {
                          error(Reasons.OTHER_LEADER, myOtherLeader);
                      }
                    }

                    _logger.info("Trying to become leader and computing high and low watermarks");
                    
                    // Best guess for a round number is the acceptor/learner
                    //
                    updateRndNumber(myLastCollect.getRndNumber());

                    // Best guess for starting sequence number is the acceptor/learner
                    //
                    _seqNum = _al.getLowWatermark();

                    // Possibility we're starting from scratch
                    //
                    if (_seqNum == LogStorage.EMPTY_LOG)
                        _seqNum = 0;

                    _stage = RECOVER;
                    emitCollect();

                } else if (isRecovery()) {
                    ++_seqNum;

                    if (_seqNum > _highWatermark) {
                        // Recovery is complete
                        //
                        notRecovery();
                        amLeader();

                        _logger.info("Recovery complete - we're leader doing begin with client value");

                        _value = _clientOp.getConsolidatedValue();
                        _stage = BEGIN;
                        process();

                    } else {
                        _value = LogStorage.NO_VALUE;
                        _stage = BEGIN;
                        emitCollect();
                    }

                } else if (isLeader()) {
                    _logger.info("Skipping collect phase - we're leader already");

                    _value = _clientOp.getConsolidatedValue();
                    ++_seqNum;

                    _stage = BEGIN;
                    process();
                }

                break;
            }

            case RECOVER : {
                // Compute low and high watermarks
                //
                long myMinSeq = -1;
                long myMaxSeq = -1;

                Iterator<PaxosMessage> myMessages = _messages.iterator();

                while (myMessages.hasNext()) {
                    PaxosMessage myMessage = myMessages.next();

                    if (myMessage.getType() == Operations.OLDROUND) {
                        /*
                         * An OldRound here indicates some other leader is present and we're only computing the
                         * range for recovery thus we just give up and exit
                         */
                        oldRound(myMessage);
                        return;
                    } else {
                        Last myLast = (Last) myMessage;

                        long myLow = myLast.getLowWatermark();
                        long myHigh = myLast.getHighWatermark();

                        /*
                         * Ignore EMPTY_LOG which means the participant has no valid watermarks
                         * as it's state is initial.
                         */
                        if (myLow != LogStorage.EMPTY_LOG) {
                            if (myLow < myMinSeq)
                                myMinSeq = myLow;
                        }

                        if (myHigh != LogStorage.EMPTY_LOG) {
                            if (myHigh > myMaxSeq) {
                                myMaxSeq = myHigh;
                            }
                        }
                    }
                }

                amRecovery();

                _lowWatermark = myMinSeq;
                _highWatermark = myMaxSeq;

                /*
                 * Collect always increments the sequence number so we must allow for that when figuring out where to
                 * start from low watermark
                 */
                if (_lowWatermark == -1)
                    _seqNum = -1;
                else
                    _seqNum = _lowWatermark - 1;

                _logger.info("Recovery started: " + _lowWatermark + " ->" + _highWatermark + " (" + _seqNum + ") ");

                _stage = COLLECT;
                process();

                break;
            }

            case BEGIN : {
                byte[] myValue = _value;

                /* 
                 * If we're not currently the leader, we'll have issued a collect and must process the responses.
                 * We'll be in recovery so we're interested in resolving any outstanding sequence numbers thus our
                 * proposal must be constrained by whatever values the acceptor/learners already know about
                 */
                if (! isLeader()) {
                    // Process _messages to assess what we do next - might be to launch a new round or to give up
                    //
                    long myMaxRound = 0;
                    Iterator<PaxosMessage> myMessages = _messages.iterator();

                    while (myMessages.hasNext()) {
                        PaxosMessage myMessage = myMessages.next();

                        if (myMessage.getType() == Operations.OLDROUND) {
                            oldRound(myMessage);
                            return;
                        } else {
                            Last myLast = (Last) myMessage;

                            if (myLast.getRndNumber() > myMaxRound) {
                                myMaxRound = myLast.getRndNumber();
                                myValue = myLast.getValue();
                            }
                        }
                    }
                }

                _value = myValue;

                emitBegin();
                _stage = SUCCESS;

                break;
            }

            case SUCCESS : {
                /*
                 * Old round message, causes start at collect or quit. If Accept messages total more than majority
                 * we're happy, send Success wait for all acks or redo collect. Note that if we receive OldRound
                 * here we haven't proposed a value (we do that when we emit success) thus if we die after this point
                 * the worst case would be an empty entry in the log with no filled in value. If we succeed here
                 * we've essentially reserved a slot in the log for the specified sequence number and it's up to us
                 * to fill it with the value we want.
                 */
                int myAcceptCount = 0;

                Iterator<PaxosMessage> myMessages = _messages.iterator();
                while (myMessages.hasNext()) {
                    PaxosMessage myMessage = myMessages.next();

                    if (myMessage.getType() == Operations.OLDROUND) {
                        oldRound(myMessage);
                        return;
                    } else {
                        myAcceptCount++;
                    }
                }

                if (myAcceptCount >= _membership.getMajority()) {
                    // Send success, wait for acks
                    //
                    emitSuccess();
                    _stage = COMMITTED;
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    emitBegin();
                    _stage = SUCCESS;
                }

                break;
            }

            case COMMITTED : {
                /*
                 * If ACK messages total more than majority we're happy otherwise try again.
                 */
                if (_messages.size() >= _membership.getMajority()) {
                    successful(Reasons.OK, null);
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    emitSuccess();
                    _stage = COMMITTED;
                }

                break;
            }

            default : throw new RuntimeException("Invalid state: " + _stage);
        }
    }

    private class HeartbeatTask extends TimerTask {
        public void run() {
            _logger.info("Sending heartbeat: " + System.currentTimeMillis());

            /*
             * Don't have to check the return value - if it's accepted we weren't active, otherwise we are and
             * no heartbeat is required. Note that a heartbeat could cause us to decide we're no longer leader
             * although that's unlikely if things are stable as no other node can become leader whilst we hold the
             * lease
             */
            submit(new Post(AcceptorLearner.HEARTBEAT, new byte[0]));
        }
    }

    /**
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        failed();

        OldRound myOldRound = (OldRound) aMessage;

        NodeId myCompetingNodeId = NodeId.from(myOldRound.getNodeId());

        updateRndNumber(myOldRound);

        /*
         * Some other node is active, we should abort if they are the leader by virtue of a larger nodeId
         */
        if (myCompetingNodeId.leads(_nodeId)) {
            _logger.info("Superior leader is active, backing down: " + myCompetingNodeId + ", " +
                    _nodeId);

            error(Reasons.OTHER_LEADER, myCompetingNodeId);
            return;
        }

        /*
         * Some other leader is active but we are superior, restart negotiations with COLLECT, note we must mark 
         * ourselves as not the leader (as has been done above) because having re-established leadership we must
         * perform recovery and then attempt to submit the client's value (if any) again.
         */
        _stage = COLLECT;
        process();
    }

    private void successful(int aReason, Object aContext) {
        _stage = EXIT;
        _completion = new Completion(aReason, _seqNum, _value, aContext);

        process();
    }

    private void error(int aReason, Object aContext) {
        _stage = ABORT;
        _completion = new Completion(aReason, _seqNum, _value, aContext);

        process();
    }

    /**
     * If we timed out or lost membership, we're potentially no longer leader and need to run recovery to get back to
     * the right sequence number.
     */
    private void failed() {
        notLeader();
        notRecovery();
    }

    private void emitCollect() {
        _messages.clear();

        PaxosMessage myMessage = new Collect(_seqNum, newRndNumber(), _nodeId.asLong());

        startInteraction();

        _logger.info("Leader sending collect: " + Long.toHexString(_seqNum));

        _transport.send(myMessage, NodeId.BROADCAST);
    }

    private void emitBegin() {
        _messages.clear();

        PaxosMessage myMessage = new Begin(_seqNum, getRndNumber(), _nodeId.asLong());

        startInteraction();

        _logger.info("Leader sending begin: " + Long.toHexString(_seqNum));

        _transport.send(myMessage, NodeId.BROADCAST);
    }

    private void emitSuccess() {
        _messages.clear();

        PaxosMessage myMessage = new Success(_seqNum, _value);

        startInteraction();

        _logger.info("Leader sending success: " + Long.toHexString(_seqNum));

        _transport.send(myMessage, NodeId.BROADCAST);
    }

    private void startInteraction() {
        _interactionAlarm = new InteractionAlarm();
        _watchdog.schedule(_interactionAlarm, _watchdogTimeout);

        _membership.startInteraction();
    }

    private class InteractionAlarm extends TimerTask {
        public void run() {
            expired();
        }
    }

    /**
     * @todo If we get ABORT, we could try a new round from scratch or make the client re-submit or .....
     */
    public void abort() {
        _logger.info("Membership requested abort: " + Long.toHexString(_seqNum));

        _interactionAlarm.cancel();
        
        synchronized(this) {
            failed();
            error(Reasons.BAD_MEMBERSHIP, null);
        }
    }

    private void expired() {
        _logger.info("Watchdog requested abort: " + Long.toHexString(_seqNum));

        synchronized(this) {
            // If there are enough messages we might get a majority continue
            //
            if (_messages.size() >= _membership.getMajority())
                process();
            else {
                failed();
                error(Reasons.VOTE_TIMEOUT, null);
            }
        }
    }

    public void allReceived() {
        _interactionAlarm.cancel();

        synchronized(this) {
            process();
        }
    }

    /**
     * Request a vote on a value.
     *
     * @param anOp is the value to attempt to agree upon
     */
    private void submit(Post anOp) {
        synchronized (this) {
            if (! isReady()) {
                _logger.info("Queued operation: " + anOp);
                _queue.add(anOp);
                return;
            }

            _clientOp = anOp;

            _logger.info("Initialising leader: " + Long.toHexString(_seqNum));

            _stage = SUBMITTED;

            process();
        }
    }

    /**
     * Used to process all core paxos protocol messages.
     *
     * @param aMessage is a message from some acceptor/learner
     * @param aNodeId is the address from which the message was sent
     */
    public void messageReceived(PaxosMessage aMessage, NodeId aNodeId) {
    	if (aMessage.getClassification() == PaxosMessage.CLIENT) {
    		submit((Post) aMessage);
    		return;
    	}
    	
        if (! isLeader()) {
        	_logger.info("This leader is not active - dumping message: " + aMessage);
        } else {
            _logger.info("Leader received message: " + aMessage);        	
        }
        
        synchronized (this) {
            if (aMessage.getSeqNum() == _seqNum) {
                _messages.add(aMessage);
                _membership.receivedResponse(aNodeId);
            } else {
                _logger.warn("Unexpected message received: " + aMessage.getSeqNum() + " (" +
                        Long.toHexString(_seqNum) + ")");
            }
        }

        _logger.info("Leader processed message: " + aMessage);
    }
}
