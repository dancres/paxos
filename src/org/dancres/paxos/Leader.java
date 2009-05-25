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
    private final long _nodeId;
    private final Transport _transport;
    private final AcceptorLearner _al;

    private long _seqNum = LogStorage.EMPTY_LOG;
    private long _rndNumber = 0;
    private byte[] _value;

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
    private int _reason = 0;

    private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

    /**
     * @param aSeqNum is the sequence number for the proposal this leader instance is responsible for
     * @param aProposerState is the proposer state to use for this proposal
     * @param aTransport is the transport to use for messages
     * @param aClientAddress is the endpoint for the client
     */
    public Leader(FailureDetector aDetector, long aNodeId, Transport aTransport, AcceptorLearner anAcceptorLearner) {
        _nodeId = aNodeId;
        _detector = aDetector;
        _transport = aTransport;
        _watchdogTimeout = _detector.getUnresponsivenessThreshold() + FAILURE_DETECTOR_GRACE_PERIOD;
        _al = anAcceptorLearner;
    }

    private long newRndNumber() {
        synchronized(this) {
            return ++_rndNumber;
        }
    }

    private void updateRndNumber(OldRound anOldRound) {
        synchronized(this) {
            _rndNumber = anOldRound.getLastRound() + 1;
        }
    }

    private boolean isRecovery() {
        synchronized(this) {
            return _isRecovery;
        }
    }

    private void amRecovery() {
        synchronized(this) {
            _isRecovery = true;
        }
    }

    private void notRecovery() {
        synchronized(this) {
            _isRecovery = false;
        }
    }

    private boolean isLeader() {
        synchronized(this) {
            return _isLeader;
        }
    }

    private void amLeader() {
        synchronized(this) {
            _isLeader = true;
        }
    }

    private void notLeader() {
        synchronized(this) {
            _isLeader = false;
        }
    }

    private long getRndNumber() {
        synchronized(this) {
            return _rndNumber;
        }
    }

    public long getNodeId() {
        return _nodeId;
    }

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we process the
     * result of a Collect in the BEGIN state which means we expect Last or OldRound and in SUCCESS state we expect ACCEPT or OLDROUND
     *
     * @todo Leader at point of recovery ought to source its initial sequence number from the acceptor learner
     * @todo Handle all cases in SUCCEESS e.g. we don't properly process/wait for all Acks
     */
    private void process() {
        switch(_stage) {
            case ABORT :
            case EXIT : {
                if (_stage == EXIT)
                    _logger.info("Leader reached good completion: " + Long.toHexString(_seqNum));
                else
                    _logger.info("Leader reached bad completion: " + Long.toHexString(_seqNum) + " " + _reason);

                _membership.dispose();

                // Acceptor/learner generates events for successful negotiation itself, we generate failures
                //
                if (_stage != EXIT) {
                    _al.signal(new Completion(_reason, _seqNum, _value));
                }

                return;
            }

            case COLLECT : {
                if ((! isLeader()) && (! isRecovery())) {
                    // Possibility we're starting from scratch
                    //
                    if (_seqNum == LogStorage.EMPTY_LOG)
                        _seqNum = 0;

                    _stage = RECOVER;
                    collect();

                } else if (isRecovery()) {
                    _value = LogStorage.NO_VALUE;
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
                        _stage = BEGIN;
                        collect();
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

                // If we're not currently the leader, we'll have issued a collect and must process the responses
                //
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

                begin();
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
                    success();
                    _stage = EXIT;
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    begin();
                    _stage = SUCCESS;
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

        long myCompetingNodeId = myOldRound.getNodeId();

        /*
         * Some other node is active, we should abort if they are the leader by virtue of a larger nodeId
         */
        if (myCompetingNodeId > _nodeId) {
            _logger.info("Superior leader is active, backing down: " + Long.toHexString(myCompetingNodeId) + ", " +
                    Long.toHexString(_nodeId));

            _stage = ABORT;
            _reason = Reasons.OTHER_LEADER;
            process();
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

    /**
     * If we timed out or lost membership, we're potentially no longer leader and need to run recovery to get back to the right
     * sequence number.
     */
    private void failed() {
        notLeader();
        notRecovery();
    }

    private void collect() {
        _messages.clear();

        PaxosMessage myMessage = new Collect(_seqNum, newRndNumber(), _nodeId);

        startInteraction();

        _logger.info("Leader sending collect: " + Long.toHexString(_seqNum));

        _transport.send(myMessage, Address.BROADCAST);
    }

    private void begin() {
        _messages.clear();

        PaxosMessage myMessage = new Begin(_seqNum, getRndNumber(), _nodeId, _value);

        startInteraction();

        _logger.info("Leader sending begin: " + Long.toHexString(_seqNum));

        _transport.send(myMessage, Address.BROADCAST);
    }

    private void success() {
        _messages.clear();

        PaxosMessage myMessage = new Success(_seqNum, _value);

        startInteraction();

        _logger.info("Leader sending success: " + Long.toHexString(_seqNum));

        _transport.send(myMessage, Address.BROADCAST);
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
        failed();

        synchronized(this) {
            _stage = ABORT;
            _reason = Reasons.BAD_MEMBERSHIP;
            process();
        }
    }

    private void expired() {
        _logger.info("Watchdog requested abort: " + Long.toHexString(_seqNum));

        failed();

        synchronized(this) {
            _stage = ABORT;
            _reason = Reasons.VOTE_TIMEOUT;
            process();
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

            // Collect will decide if it can skip straight to a begin
            //
            _stage = COLLECT;

            _membership = _detector.getMembers(this);

            _logger.info("Got membership for leader: " + Long.toHexString(_seqNum) + ", (" + _membership.getSize() + ")");

            process();
        }

        return ACCEPTED;
    }

    /**
     * Used to process all core paxos protocol messages.
     *
     * @param aMessage is a message from some acceptor/learner
     * @param anAddress is the address from which the message was sent
     */
    public void messageReceived(PaxosMessage aMessage, Address anAddress) {
        _logger.info("Leader received message: " + aMessage);

        synchronized (this) {
            if (aMessage.getSeqNum() == _seqNum) {
                _messages.add(aMessage);
                _membership.receivedResponse(anAddress);
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
