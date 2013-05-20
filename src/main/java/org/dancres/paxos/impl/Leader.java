package org.dancres.paxos.impl;

import org.dancres.paxos.Completion;
import org.dancres.paxos.Proposal;
import org.dancres.paxos.VoteOutcome;
import org.dancres.paxos.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Implements the leader state machine for a specific instance of Paxos. Leader is fail fast in that the first time
 * it spots a problem, it will cease any further attempts to drive progress reporting the issue and leaving user-code
 * to re-submit a request. This applies equally to handling conflicting values (that might occur as the result of a
 * need to drive a previous instance to completion as the result of LAST responses).
 *
 * @todo Add a test for validating retries on dropped packets in later leader states.
 *
 * @author dan
 */
class Leader implements MembershipListener, Instance {
    
    private static final Logger _logger = LoggerFactory.getLogger(Leader.class);

    private static final long GRACE_PERIOD = 1000;

    private static final long MAX_TRIES = 3;

    private final Common _common;
    private final LeaderFactory _factory;

    private final long _seqNum;
    private final long _rndNumber;
    private long _tries = 0;

    private Proposal _prop;
    private Completion<VoteOutcome> _submitter;
    private Completion<Leader> _leaderReceiver;

    /**
     * This alarm is used to limit the amount of time the leader will wait for responses from all apparently live
     * members in a round of communication.
     */
    private TimerTask _interactionAlarm;

    /**
     * Tracks membership for an entire paxos instance.
     */
    private Membership _membership;

    private final State _startState;
    private State _currentState = State.INITIAL;

    /**
     * In cases of ABORT, indicates the reason
     */
    private final Deque<VoteOutcome> _outcomes = new LinkedBlockingDeque<VoteOutcome>();

    private final List<Transport.Packet> _messages = new ArrayList<Transport.Packet>();

    Leader(Common aCommon, LeaderFactory aFactory) {
        _common = aCommon;
        _factory = aFactory;
        _startState = State.COLLECT;
        _seqNum = _common.getLowWatermark().getSeqNum() + 1;
        _rndNumber = _common.getLeaderRndNum() + 1;        
    }

    private Leader(Common aCommon, LeaderFactory aFactory,
                  long aNextSeq, long aRndNumber, State aStartState) {
        _common = aCommon;
        _factory = aFactory;
        _seqNum = aNextSeq;
        _rndNumber = aRndNumber;
        _startState = aStartState;
    }

    Deque<VoteOutcome> getOutcomes() {
        return new LinkedList<VoteOutcome>(_outcomes);
    }

    void shutdown() {
    	synchronized(this) {
            if ((! isDone()) && (_currentState != State.SHUTDOWN)) {
    		    _currentState = State.SHUTDOWN;
                _outcomes.clear();
                process();
            }
    	}
    }

    public long getRound() {
        return _rndNumber;
    }

    public long getSeqNum() {
        return _seqNum;
    }

    public State getState() {
        synchronized(this) {
            return _currentState;
        }
    }

    private boolean isDone() {
        synchronized(this) {
            return ((_currentState.equals(State.EXIT)) || (_currentState.equals(State.ABORT)));
        }
    }

    private void cleanUp() {
     _messages.clear();

     if (_membership != null)
         _membership.dispose();       
    }

    private void reportOutcome() {
        /*
         * First outcome is always the one we report to the submitter even if there are others (available via
         * getOutcomes()). Multiple outcomes occur when we detect a previously proposed value and must drive it
         * to completion. The originally submitted value will need re-submitting. Hence submitter is told
         * OTHER_VALUE whilst AL listeners will see DECISION containing the previously proposed value.
         */
        _submitter.complete(_outcomes.getFirst());
        followUp();
    }

    private void followUp() {
        if ((! isDone()) || (_leaderReceiver == null))
            return;
        else
            _leaderReceiver.complete(constructFollowing());
    }

    private Leader constructFollowing() {
        switch(_outcomes.getLast().getResult()) {

                /*
                 * We can apply multi-paxos. Next sequence number and round number needn't change.
                 */
            case VoteOutcome.Reason.DECISION : {
                return new Leader(_common, _factory, _outcomes.getLast().getSeqNum() + 1,
                        _outcomes.getLast().getRndNumber(), State.BEGIN);
            }

                /*
                 * Other leader, in which case we use the values returned from the objecting AL as the basis
                 * of our next try. We can receive OTHER_LEADER to indicate an active leader conflict or that
                 * we're too out of date (our sequence number is <= AL.low_watermark). In the case of the latter
                 * the OLD_ROUND contains the low watermark sequence number and last successful round. We therefore
                 * need to propose at +1 on both sequence number and round if we are to stand a chance of succeeding.
                 */
            case VoteOutcome.Reason.OTHER_LEADER : {
                return new Leader(_common, _factory, _outcomes.getLast().getSeqNum() + 1,
                        _outcomes.getLast().getRndNumber() + 1, State.COLLECT);
            }

                /*
                 * Haven't made any progress, try the same sequence and round again.
                 */
            case VoteOutcome.Reason.BAD_MEMBERSHIP :
            case VoteOutcome.Reason.VOTE_TIMEOUT : {
                return new Leader(_common, _factory,
                        _outcomes.getLast().getSeqNum(), _outcomes.getLast().getRndNumber(), State.COLLECT);
            }

            default :
                throw new IllegalStateException("Unrecognised outcome");
        }
    }

    /**
     * Get the next leader in the chain. Non-blocking, completion is called at the point a leader can be constructed
     */
    void nextLeader(Completion<Leader> aCompletion) {
        synchronized(this) {
            if (_leaderReceiver != null)
                throw new IllegalStateException("Completion for leader already present");

            _leaderReceiver = aCompletion;

            followUp();
        }
    }

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we
     * process the result of a Collect in the BEGIN state which means we expect Last or OldRound and in LEARNED state
     * we expect ACCEPT or OLDROUND
     *
     * @todo Shutdown needs sorting in the context of chained leaders (which can't happen whilst nextLeader
     * is a blocking implementation).
     */
    private void process() {
        switch (_currentState) {
            case SHUTDOWN : {
                _logger.info(stateToString());
                
                cleanUp();

                if (_interactionAlarm != null)
                    cancelInteraction();

                _currentState = State.ABORT;

                return;
            }

            case ABORT : {
                _logger.info(stateToString() + " : " + _outcomes);

                cancelInteraction();

                cleanUp();

                _factory.dispose(this);

                reportOutcome();
                
                return;
            }

            case EXIT : {
            	_logger.info(stateToString() + " : " + _outcomes);

                cleanUp();

                _factory.dispose(this);

                reportOutcome();

                return;
            }

            case SUBMITTED : {
                _tries = 0;
                _membership = _common.getPrivateFD().getMembers(this);

                _logger.debug(stateToString() + " : got membership: (" +
                        _membership.getSize() + ")");

                _currentState = _startState;
                process();

                break;
            }

            /*
             * It's possible an AL will have seen a success that no others saw such that a previous value is 
             * not fully committed. That's okay as a lagging leader will propose a new client value for that sequence
             * number and find that AL tells it about this value which will cause the leader to finish off that
             * round and any others after which it can propose the client value for a sequence number. Should that AL
             * die the record is lost and the client needs to re-propose the value.
             * 
             * Other AL's may have missed other values, that's also okay as they will separately deduce they have
             * missing instances to catch-up and recover that state from those around them.
             */
            case COLLECT : {
                emit(new Collect(_seqNum, _rndNumber));
                _currentState = State.BEGIN;

            	break;
            }

            case BEGIN : {
                Transport.Packet myLast = null;

                for(Transport.Packet p : _messages) {                    
                    Last myNewLast = (Last) p.getMessage();

                    if (! myNewLast.getConsolidatedValue().equals(Proposal.NO_VALUE)) {
                        if (myLast == null)
                            myLast = p;
                        else if (myNewLast.getRndNumber() > ((Last) myLast.getMessage()).getRndNumber()) {
                            myLast = p;
                        }
                    }
                }

                /*
                 * If we have a value from a LAST message and it's not the same as the one we want to propose,
                 * we've hit an outstanding paxos instance and must now drive it to completion. Note we must
                 * compare the consolidated value we want to propose as the one in the LAST message will be a
                 * consolidated value.
                 */
                if ((myLast != null) && (! ((Last) myLast.getMessage()).getConsolidatedValue().equals(_prop))) {
                    VoteOutcome myOutcome = new VoteOutcome(VoteOutcome.Reason.OTHER_VALUE,
                            _seqNum, _rndNumber, _prop, myLast.getSource());
                    _outcomes.add(myOutcome);

                    _prop = ((Last) myLast.getMessage()).getConsolidatedValue();
                }

                emit(new Begin(_seqNum, _rndNumber, _prop));
                _currentState = State.SUCCESS;

                break;
            }

            case SUCCESS : {
                if (_messages.size() >= _common.getPrivateFD().getMajority()) {
                    // Send success
                    //
                    emit(new Learned(_seqNum, _rndNumber));
                    cancelInteraction();
                    successful(VoteOutcome.Reason.DECISION);
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    emit(new Begin(_seqNum, _rndNumber, _prop));
                }

                break;
            }

            default : throw new Error("Invalid state: " + _currentState);
        }
    }

    /**
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        OldRound myOldRound = (OldRound) aMessage;

        InetSocketAddress myCompetingNodeId = myOldRound.getLeaderNodeId();

        _logger.info(stateToString() + ": Another leader is active, backing down: " + myCompetingNodeId + " (" +
                Long.toHexString(myOldRound.getLastRound()) + ", " + Long.toHexString(_rndNumber) + ")");

        _currentState = State.ABORT;
        _outcomes.add(new VoteOutcome(VoteOutcome.Reason.OTHER_LEADER, myOldRound.getSeqNum(),
                myOldRound.getLastRound(), _prop, myCompetingNodeId));

        process();
    }

    private void successful(int aReason) {
        _currentState = State.EXIT;
        _outcomes.add(new VoteOutcome(aReason, _seqNum, _rndNumber, _prop,
                _common.getTransport().getLocalAddress()));

        process();
    }

    private void error(int aReason) {
    	error(aReason, _common.getTransport().getLocalAddress());
    }
    
    private void error(int aReason, InetSocketAddress aLeader) {
        _currentState = State.ABORT;
        _outcomes.add(new VoteOutcome(aReason, _seqNum, _rndNumber, _prop, aLeader));
        
        _logger.info(stateToString() + " : " + _outcomes);

        process();
    }

    private void emit(PaxosMessage aMessage) {
        _messages.clear();

        if (startInteraction()) {
            _logger.info(stateToString() + " : " + aMessage);

            _common.getTransport().send(aMessage, _common.getTransport().getBroadcastAddress());
        }
    }

    private boolean startInteraction() {
        assert _interactionAlarm == null;

        _interactionAlarm = new TimerTask() {
            public void run() {
                expired();
            }
        };

        _common.getWatchdog().schedule(_interactionAlarm, GRACE_PERIOD);

        return _membership.startInteraction();
    }

    public void abort() {
        _logger.info(stateToString() + " : Membership requested abort");

        synchronized(this) {
            error(VoteOutcome.Reason.BAD_MEMBERSHIP);
        }
    }

    public void allReceived() {
        synchronized(this) {
            cancelInteraction();

            _tries = 0;
            process();
        }
    }

    private void cancelInteraction() {
        assert _interactionAlarm != null;

        _interactionAlarm.cancel();
        _common.getWatchdog().purge();
        _interactionAlarm = null;
    }

    private void expired() {
        _logger.info(stateToString() + " : Watchdog requested abort: ");

        synchronized(this) {
            if (_currentState.equals(State.SUCCESS)) {
                ++_tries;

                if (_tries < MAX_TRIES) {
                	cancelInteraction();
                    process();
                    return;
                }
            }

            error(VoteOutcome.Reason.VOTE_TIMEOUT);
        }
    }

    /**
     * Request a vote on a value.
     *
     * Completion is for cleanup steps only. DO NOT ATTEMPT TO INITIATE FURTHER ROUNDS FROM WITHIN THIS CALLBACK AS
     * THEY WILL LIKELY DEADLOCK
     *
     * @todo Remove the deadlocks (see method comment)
     *
     * @param aValue is the value to attempt to agree upon
     */
    public void submit(Proposal aValue, Completion<VoteOutcome> aSubmitter) {
        synchronized (this) {
            if (_currentState != State.INITIAL)
                throw new IllegalStateException("Submit already done, create another leader");

            if (aSubmitter == null)
                throw new IllegalArgumentException("Submitter cannot be null");

            _logger.info(stateToString());

            _submitter = aSubmitter;
            _prop = aValue;

            _currentState = State.SUBMITTED;

            process();
        }
    }

    /**
     * Used to process all core paxos protocol messages.
     *
     * We optimise by counting messages and transitioning as soon as we have enough and detecting failure
     * immediately. But what if we miss an oldRound? If we miss an OldRound it can only be because a minority is seeing
     * another leader and when it runs into our majority, it will be forced to resync seqNum/learnedValues etc. In
     * essence if we've progressed through enough phases to get a majority commit we can go ahead and set the value as
     * any future leader wading in will pick up our value. NOTE: This optimisation requires the membership impl to
     * understand the concept of minimum acceptable majority.
     *
     * @todo Update OldRound handling - if we track all OldRounds and pick the highest by round and sequence number
     * we can potentially accelerate recovery and reduce client disruption
     */
    public void messageReceived(Transport.Packet aPacket) {
        PaxosMessage myMessage = aPacket.getMessage();

        assert (myMessage.getClassification() != PaxosMessage.CLIENT): "Got a client message and shouldn't have done";

        synchronized (this) {
            switch (_currentState) {
                case ABORT :
                case EXIT :
                case SHUTDOWN : {
                    return;
                }
            }

            _logger.info(stateToString() + " : " + myMessage);

            if (myMessage instanceof LeaderSelection) {
                if (((LeaderSelection) myMessage).routeable(this)) {
                    if (myMessage instanceof OldRound) {
                        oldRound(myMessage);
                    } else {
                        _messages.add(aPacket);
                        _membership.receivedResponse(aPacket.getSource());
                    }
                    return;
                }
            }

            _logger.warn(stateToString() + ": Unexpected message received: " + myMessage);
        }
    }

    String stateToString() {
        State myState = getState();

        return _common.getTransport().getLocalAddress() +
                ": (" + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNumber) + ")" + " : " + myState +
                " tries: " + _tries + "/" + MAX_TRIES;
    }

    public String toString() {
    	return "Leader: " + stateToString();
    }
}
