package org.dancres.paxos;

import org.dancres.paxos.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implements the leader state machine.
 *
 * @author dan
 */
public class Leader implements MembershipListener {
    private static final Logger _logger = LoggerFactory.getLogger(Leader.class);

    /*
     * Used to compute the timeout period for watchdog tasks.  In order to behave sanely we want the failure detector to be given
     * the best possible chance of detecting problems with the members.  Thus the timeout for the watchdog is computed as the
     * unresponsiveness threshold of the failure detector plus a grace period.
     */
    private static final long FAILURE_DETECTOR_GRACE_PERIOD = 2000;

    /*
     * Internal states
     */
    private static final int COLLECT = 0;
    private static final int BEGIN = 1;
    private static final int SUCCESS = 2;
    private static final int EXIT = 3;
    private static final int ABORT = 4;
    private static final int RECOVER = 5;
    private static final int COMMITTED = 6;
    private static final int SUBMITTED = 7;

    /**
     * Indicates the Leader is not ready to process the passed message and the caller should retry.
     */
    public static final int BUSY = 256;

    /**
     * Indicates the Leader has accepted the message and is waiting for further messages.
     */
    public static final int ACCEPTED = 257;

    private final Timer _watchdog = new Timer("Leader watchdog");
    private final long _watchdogTimeout;
    private final FailureDetector _detector;
    private final NodeId _nodeId;
    private final Transport _transport;
    private final AcceptorLearner _al;

    private long _seqNum = LogStorage.EMPTY_LOG;

    /**
     * Tracks the round number this leader used last. This cannot be stored in the acceptor/learner's version without risking breaking
     * the collect protocol (the al could suddenly believe it's seen a collect it hasn't and become noisy when it should be silent).
     * This field can be updated as the result of OldRound messages.
     */
    private long _rndNumber = 0;

    /**
     * The current value the leader is using for a proposal. Might be sourced from _clientOp but can also come from Last messages as part
     * of recovery.
     */
    private byte[] _value;

    private boolean _checkLeader = true;

    /**
     * Note that being the leader is merely an optimisation and saves on sending COLLECTs.  Thus if one thread establishes we're leader and
     * a prior thread decides otherwise with the latter being last to update this variable we'll simply do an unnecessary COLLECT.  The
     * protocol execution will still be correct.
     */
    private boolean _isLeader = false;

    /**
     * When in recovery mode we perform collect for each sequence number in the recovery range
     */
    private boolean _isRecovery = false;

    private long _lowWatermark;
    private long _highWatermark;

    /**
     * Maintains the current client request. The actual sequence number and value the state machine operates on are held in
     * <code>_seqNum</code> and <code>_value</code> and during recovery will not be the same as the client request.  Thus we cache
     * the client request and move it into the operating variables once recovery is complete.
     */
    private Operation _clientOp;

    private TimerTask _activeAlarm;

    private Membership _membership;

    private int _stage = EXIT;
    
    /**
     * In cases of ABORT, indicates the reason
     */
    private Completion _completion;

    private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

    public Leader(FailureDetector aDetector, NodeId aNodeId, Transport aTransport, AcceptorLearner anAcceptorLearner) {
        _nodeId = aNodeId;
        _detector = aDetector;
        _transport = aTransport;
        _watchdogTimeout = _detector.getUnresponsivenessThreshold() + FAILURE_DETECTOR_GRACE_PERIOD;
        _al = anAcceptorLearner;
    }

    public void setLeaderCheck(boolean aCheck) {
        _logger.warn("Setting leader check: " + aCheck);

        synchronized(this) {
            _checkLeader = aCheck;
        }
    }

    private long getRndNumber() {
        return _rndNumber;
    }

    private long newRndNumber() {
        return ++_rndNumber;
    }

    private void updateRndNumber(OldRound anOldRound) {
        _rndNumber = anOldRound.getLastRound() + 1;
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

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we process the
     * result of a Collect in the BEGIN state which means we expect Last or OldRound and in SUCCESS state we expect ACCEPT or OLDROUND
     */
    private void process() {
        switch(_stage) {
            case ABORT :
            case EXIT : {
                _logger.info("Leader reached " + _completion);

                if (_membership != null)
                    _membership.dispose();

                // Acceptor/learner generates events for successful negotiation itself, we generate failures
                //
                if (_stage != EXIT) {
                    _al.signal(_completion);
                }

                return;
            }

            case SUBMITTED : {
                if (_checkLeader && !_detector.amLeader(_nodeId)) {
                    failed();
                    error(Reasons.OTHER_LEADER, _detector.getLeader());
                    return ;
                }

                _membership = _detector.getMembers(this);

                _logger.info("Got membership for leader: " + Long.toHexString(_seqNum) + ", (" + _membership.getSize() + ")");

                // Collect will decide if it can skip straight to a begin
                //
                _stage = COLLECT;
                process();

                break;
            }

            case COLLECT : {
                if ((! isLeader()) && (! isRecovery())) {
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

                        _value = _clientOp.getValue();
                        _stage = BEGIN;
                        process();

                    } else {
                        _value = LogStorage.NO_VALUE;
                        _stage = BEGIN;
                        emitCollect();
                    }

                } else if (isLeader()) {
                    _logger.info("Skipping collect phase - we're leader already");

                    _value = _clientOp.getValue();
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
                        oldRound(myMessage);
                        return;
                    } else {
                        Last myLast = (Last) myMessage;

                        long myLow = myLast.getLowWatermark();
                        long myHigh = myLast.getHighWatermark();

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

                // Collect always increments the sequence number so we must allow for that when figuring out where to start from low watermark
                //
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
                 * Old round message, causes start at collect or quit.
                 * If Accept messages total more than majority we're happy, send Success wait for all acks
                 * or redo collect
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
                 * Old round message, causes start at collect or quit.
                 * If ACK messages total more than majority we're happy, send Success wait for all acks
                 * or redo collect
                 */
                int myAckCount = 0;

                Iterator<PaxosMessage> myMessages = _messages.iterator();
                while (myMessages.hasNext()) {
                    PaxosMessage myMessage = myMessages.next();

                    if (myMessage.getType() == Operations.OLDROUND) {
                        oldRound(myMessage);
                        return;
                    } else {
                        myAckCount++;
                    }
                }

                if (myAckCount >= _membership.getMajority()) {
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

    /**
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        failed();

        OldRound myOldRound = (OldRound) aMessage;

        NodeId myCompetingNodeId = NodeId.from(myOldRound.getNodeId());

        /*
         * Some other node is active, we should abort if they are the leader by virtue of a larger nodeId
         */
        if (myCompetingNodeId.leads(_nodeId)) {
            _logger.info("Superior leader is active, backing down: " + myCompetingNodeId + ", " +
                    _nodeId);

            error(Reasons.OTHER_LEADER, new Long(myCompetingNodeId.asLong()));
            return;
        }

        updateRndNumber(myOldRound);

        /*
         * Some other leader is active but we are superior, restart negotiations with COLLECT, note we must mark ourselves as
         * not the leader (as has been done above) because having re-established leadership we must perform recovery and
         * then attempt to submit the client's value (if any) again.
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
     * If we timed out or lost membership, we're potentially no longer leader and need to run recovery to get back to the right
     * sequence number.
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
        _activeAlarm = new Alarm();
        _watchdog.schedule(_activeAlarm, _watchdogTimeout);

        _membership.startInteraction();
    }

    /**
     * @todo If we get ABORT, we could try a new round from scratch or make the client re-submit or .....
     */
    public void abort() {
        _logger.info("Membership requested abort: " + Long.toHexString(_seqNum));

        _activeAlarm.cancel();
        
        synchronized(this) {
            failed();
            error(Reasons.BAD_MEMBERSHIP, null);
        }
    }

    private void expired() {
        _logger.info("Watchdog requested abort: " + Long.toHexString(_seqNum));

        synchronized(this) {
            failed();
            error(Reasons.VOTE_TIMEOUT, null);
        }
    }

    public void allReceived() {
        _activeAlarm.cancel();

        synchronized(this) {
            process();
        }
    }

    /**
     * Request a vote on a value.
     *
     * @param anOp is the value to attempt to agree upon
     * @return BUSY if the state machine is already attempting to get a decision on some vote other ACCEPTED.
     */
    public int submit(Operation anOp) {
        synchronized (this) {
            if ((_stage != ABORT) && (_stage != EXIT)) {
                return BUSY;
            }

            _clientOp = anOp;

            _logger.info("Initialising leader: " + Long.toHexString(_seqNum));

            _stage = SUBMITTED;

            process();
        }

        return ACCEPTED;
    }

    /**
     * Used to process all core paxos protocol messages.
     *
     * @param aMessage is a message from some acceptor/learner
     * @param aNodeId is the address from which the message was sent
     */
    public void messageReceived(PaxosMessage aMessage, NodeId aNodeId) {
        _logger.info("Leader received message: " + aMessage);

        synchronized (this) {
            if (aMessage.getSeqNum() == _seqNum) {
                _messages.add(aMessage);
                _membership.receivedResponse(aNodeId);
            } else {
                _logger.warn("Unexpected message received: " + aMessage.getSeqNum() + " (" + Long.toHexString(_seqNum) + ")");
            }
        }

        _logger.info("Leader processed message: " + aMessage);
    }

    private class Alarm extends TimerTask {
        public void run() {
            expired();
        }
    }
}
