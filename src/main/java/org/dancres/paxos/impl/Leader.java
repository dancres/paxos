package org.dancres.paxos.impl;

import org.dancres.paxos.*;
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
 * TODO: Add a test for validating retries on dropped packets in later leader states.
 *
 * @author dan
 */
class Leader implements Instance {

    static class LeaseDuration {
        private static final long DEFAULT_LEADER_LEASE = 30000;
        private static volatile long _leaderLease = DEFAULT_LEADER_LEASE;

        public static void set(long aDuration) {
            _leaderLease = aDuration;
        }

        public static long get() {
            return _leaderLease;
        }
    }

    private static final Logger _logger = LoggerFactory.getLogger(Leader.class);

    private static final List<Transport.Packet> NO_MESSAGES = Collections.emptyList();
    private static final long GRACE_PERIOD = 1000;

    private static final long MAX_TRIES = 3;

    private final Common _common;
    private final long _seqNum;
    private final long _rndNumber;
    private long _tries = 0;

    private Proposal _prop;
    private Completion<Leader> _submitter;

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
    private StateMachine _stateMachine = new StateMachine();

    /**
     * In cases of ABORT, indicates the reason
     */
    private final Deque<VoteOutcome> _outcomes = new LinkedBlockingDeque<>();

    private final List<Transport.Packet> _messages = new ArrayList<>();

    private class StateMachine {
        private Map<State, Set<State>> _acceptableTransitions;

        private StateMachine() {
            Map<State, Set<State>> myAccTrans = new HashMap<>();

            myAccTrans.put(State.INITIAL, new HashSet<>(Arrays.asList(State.SUBMITTED, State.SHUTDOWN)));
            myAccTrans.put(State.SUBMITTED, new HashSet<>(Arrays.asList(State.BEGIN, State.COLLECT, State.ABORT)));
            myAccTrans.put(State.COLLECT, new HashSet<>(Arrays.asList(State.BEGIN)));
            myAccTrans.put(State.BEGIN, new HashSet<>(Arrays.asList(State.SUCCESS, State.ABORT)));
            myAccTrans.put(State.SUCCESS, new HashSet<>(Arrays.asList(State.ABORT, State.EXIT)));
            myAccTrans.put(State.EXIT, new HashSet<>(Arrays.asList(State.SHUTDOWN)));
            myAccTrans.put(State.ABORT, new HashSet<>(Arrays.asList(State.SHUTDOWN)));
            myAccTrans.put(State.SHUTDOWN, new HashSet<State>());

            _acceptableTransitions = Collections.unmodifiableMap(myAccTrans);
        }

        private State _currentState = State.INITIAL;

        void transition(State aNewState) {
            if (_acceptableTransitions.get(_currentState).contains(aNewState)) {
                Leader._logger.debug(Leader.this.toString() + " " + _currentState + " -> " + aNewState);

                _currentState = aNewState;
            } else {
                throw new Error("Illegal state transition " +
                        _currentState + ", " + aNewState + " -> " + _acceptableTransitions.get(_currentState));
            }
        }

        State getCurrentState() {
            return _currentState;
        }

        boolean isDone() {
            return ((_currentState.equals(State.EXIT)) || (_currentState.equals(State.ABORT)));
        }
    }

    Leader(Common aCommon, Instance anInstance) {
        _common = aCommon;
        _seqNum = anInstance.getSeqNum();
        _rndNumber = anInstance.getRound();
        _startState = anInstance.getState();
    }

    void shutdown() {
    	synchronized(this) {
            if ((! _stateMachine.isDone()) && (_stateMachine.getCurrentState() != State.SHUTDOWN)) {
    		    _stateMachine.transition(State.SHUTDOWN);
                _outcomes.clear();

                process(NO_MESSAGES);
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
            return _stateMachine.getCurrentState();
        }
    }

    Deque<VoteOutcome> getOutcomes() {
        return _outcomes;
    }

    private void reportOutcome() {
        _submitter.complete(this);
    }

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we
     * process the result of a Collect in the BEGIN state which means we expect Last or OldRound and in LEARNED state
     * we expect ACCEPT or OLDROUND
     */
    private void process(List<Transport.Packet> aMessages) {
        switch (_stateMachine.getCurrentState()) {
            case SHUTDOWN : {
                _logger.debug(toString() + " Shutdown");
                
                if (_interactionAlarm != null)
                    cancelInteraction();

                return;
            }

            case ABORT : {
                _logger.debug(toString() + " Abort: " + _outcomes);

                if (_interactionAlarm != null)
                    cancelInteraction();

                reportOutcome();
                
                return;
            }

            case EXIT : {
            	_logger.debug(toString() + " Exit: " + _outcomes);

                reportOutcome();

                return;
            }

            case SUBMITTED : {
                if (! _membership.couldComplete()) {
                    error(VoteOutcome.Reason.BAD_MEMBERSHIP);
                } else if (! _common.amMember()) {
                    error(VoteOutcome.Reason.NOT_MEMBER);
                } else {
                    _stateMachine.transition(_startState);
                    process(NO_MESSAGES);
                }

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
                _stateMachine.transition(State.BEGIN);

            	break;
            }

            case BEGIN : {
                Transport.Packet myLast = null;

                for(Transport.Packet p : aMessages) {
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
                _stateMachine.transition(State.SUCCESS);

                break;
            }

            case SUCCESS : {
                if (aMessages.size() >= _common.getTransport().getFD().getMajority()) {
                    // AL's monitor each other's accepts so auto-commit for themselves, no need to send a confirmation
                    //
                    successful(VoteOutcome.Reason.VALUE);
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    emit(new Begin(_seqNum, _rndNumber, _prop));
                }

                break;
            }

            default : throw new Error("Invalid state: " + _stateMachine.getCurrentState());
        }
    }

    /**
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        OldRound myOldRound = (OldRound) aMessage;

        InetSocketAddress myCompetingNodeId = myOldRound.getLeaderNodeId();

        _logger.warn(toString() + " Other leader active, backing down: " + myCompetingNodeId + " (" +
                Long.toHexString(myOldRound.getLastRound()) + ", " + Long.toHexString(_rndNumber) + ")");

        _stateMachine.transition(State.ABORT);
        _outcomes.add(new VoteOutcome(VoteOutcome.Reason.OTHER_LEADER, myOldRound.getSeqNum(),
                myOldRound.getLastRound(), _prop, myCompetingNodeId));
        _common.signal(new StateEvent(StateEvent.Reason.NEW_LEADER, myOldRound.getSeqNum(), myOldRound.getLastRound(),
                Proposal.NO_VALUE, _common.getTransport().getFD().dataForNode(myCompetingNodeId),
                myCompetingNodeId));

        process(NO_MESSAGES);
    }

    private void successful(int aReason) {
        _stateMachine.transition(State.EXIT);
        _outcomes.add(new VoteOutcome(aReason, _seqNum, _rndNumber, _prop,
                _common.getTransport().getLocalAddress()));

        process(NO_MESSAGES);
    }

    private void error(int aReason) {
    	error(aReason, _common.getTransport().getLocalAddress());
    }
    
    private void error(int aReason, InetSocketAddress aLeader) {
        _stateMachine.transition(State.ABORT);
        _outcomes.add(new VoteOutcome(aReason, _seqNum, _rndNumber, _prop, aLeader));
        
        _logger.error(toString() + " : " + _outcomes);

        process(NO_MESSAGES);
    }

    private void emit(PaxosMessage aMessage) {
        startInteraction();

        _logger.debug(toString() + " : " + aMessage);

        _common.getTransport().send(_common.getTransport().getPickler().newPacket(aMessage),
                _common.getTransport().getBroadcastAddress());
    }

    private void startInteraction() {
        assert _interactionAlarm == null;

        _interactionAlarm = new TimerTask() {
            public void run() {
                expired();
            }
        };

        _common.getWatchdog().schedule(_interactionAlarm, GRACE_PERIOD);
    }

    private void cancelInteraction() {
        assert _interactionAlarm != null;

        _interactionAlarm.cancel();
        _common.getWatchdog().purge();
        _interactionAlarm = null;
    }

    private void expired() {
        synchronized(this) {
            _logger.debug(toString() + " Watchdog requested abort: ");

            switch (_stateMachine.getCurrentState()) {
                case SUCCESS : {
                    ++_tries;

                    if (_tries < MAX_TRIES) {
                        cancelInteraction();
                        process(_messages);
                        _messages.clear();
                    } else {
                        error(VoteOutcome.Reason.VOTE_TIMEOUT);
                    }

                    break;
                }

                case EXIT :
                case ABORT :
                case SHUTDOWN : {
                    break;
                }

                default : {
                    error(VoteOutcome.Reason.VOTE_TIMEOUT);
                    break;
                }
            }
        }
    }

    /**
     * Request a vote on a value.
     *
     * Completion is for cleanup steps only. DO NOT ATTEMPT TO INITIATE FURTHER ROUNDS FROM WITHIN THIS CALLBACK AS
     * THEY WILL LIKELY DEADLOCK
     *
     * TODO: Remove the deadlocks (see method comment)
     *
     * @param aValue is the value to attempt to agree upon
     */
    void submit(Proposal aValue, Completion<Leader> aSubmitter) {
        _logger.debug(toString() + " (" + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNumber) + ")");

        synchronized (this) {
            if (_stateMachine.getCurrentState() != State.INITIAL)
                throw new IllegalStateException("Submit already done, create another leader");

            if (aSubmitter == null)
                throw new IllegalArgumentException("Submitter cannot be null");

            _submitter = aSubmitter;
            _prop = aValue;

            _tries = 0;
            _stateMachine.transition(State.SUBMITTED);

            _membership = _common.getTransport().getFD().getMembers();

            _logger.trace(toString() + " got membership: (" +
                    _membership.getSize() + ")");

            process(NO_MESSAGES);
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
     * TODO: Update OldRound handling - if we track all OldRounds and pick the highest by round and sequence number
     * we can potentially accelerate recovery and reduce client disruption
     */
    void processMessage(Transport.Packet aPacket) {
        PaxosMessage myMessage = aPacket.getMessage();

        assert (! myMessage.getClassifications().contains(PaxosMessage.Classification.CLIENT)): "Got a client message and shouldn't have done";

        synchronized (this) {
            switch (_stateMachine.getCurrentState()) {
                case ABORT :
                case EXIT :
                case SHUTDOWN : {
                    return;
                }
            }

            _logger.debug(toString() + " " + aPacket);

            if (myMessage instanceof LeaderSelection) {
                if (((LeaderSelection) myMessage).routeable(this)) {
                    if (myMessage.getType() == PaxosMessage.Types.OLDROUND) {
                        oldRound(myMessage);
                    } else {
                        _messages.add(aPacket);

                        Set<InetSocketAddress> myRespondingAddresses = new HashSet<>();

                        for (Transport.Packet myPacket : _messages)
                            myRespondingAddresses.add(myPacket.getSource());

                        if (_membership.isMajority(myRespondingAddresses)) {
                            cancelInteraction();

                            _tries = 0;
                            process(_messages);
                            _messages.clear();
                        }
                    }

                    return;
                }
            } else {
                _logger.trace(toString() + " Dropped message (didn't route) " + myMessage);
            }
        }
    }

    public String toString() {
        return "LD [ " + _common.getTransport().getLocalAddress() + " ]";
    }
}
