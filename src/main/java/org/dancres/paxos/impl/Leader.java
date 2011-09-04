package org.dancres.paxos.impl;

import org.dancres.paxos.Proposal;
import org.dancres.paxos.Event;
import org.dancres.paxos.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Implements the leader state machine.
 *
 * @todo Add a test for validating multiple sequence number recovery.
 * @todo Add a test for validating retries on dropped packets in later leader states.
 *
 * @author dan
 */
public class Leader implements MembershipListener {
    private static final Logger _logger = LoggerFactory.getLogger(Leader.class);

    private static final long GRACE_PERIOD = 1000;

    private static final long MAX_TRIES = 3;

    /**
     * Leader reaches COLLECT after SUBMITTED. If already leader, a transition to BEGIN will be immediate.
     * If not leader and recovery is not active, move to state RECOVER otherwise leader is in recovery and must now
     * settle low to high watermark (as set by RECOVER) before processing any submitted value by repeatedly executing
     * full instances of paxos (including a COLLECT to recover any previous value that was proposed).
     * 
     * In BEGIN we attempt to reserve a slot in the sequence of operations. Transition to SUCCESS after emitting begin 
     * to see if the slot was granted.
     * 
     * In SUCCESS, Leader has sent a BEGIN and now determines if it has secured the slot associated with the sequence 
     * number. If the slot was secured, a value will be sent to all members of the current instance after which there
     * will be a transition to COMMITTED.
     * 
     * In EXIT a paxos instance was completed successfully, clean up is all that remains.
     * 
     * In ABORT a paxos isntance failed for some reason (which will be found in </code>_completion</code>).
     * 
     * In SUBMITTED, Leader has been given a value and should attempt to complete a paxos instance.
     */
    enum States {
    	COLLECT, BEGIN, SUCCESS, EXIT, ABORT, SUBMITTED
    };
    

    private final Timer _watchdog = new Timer("Leader timers");
    private final FailureDetector _detector;
    private final Transport _transport;
    private final AcceptorLearner _al;

    private long _seqNum = Constants.UNKNOWN_SEQ;

    /**
     * Tracks the round number this leader used last. This cannot be stored in the acceptor/learner's version without
     * risking breaking the collect protocol (the al could suddenly believe it's seen a collect it hasn't and become
     * noisy when it should be silent). This field can be updated as the result of OldRound messages.
     */
    private long _rndNumber = 0;

    private long _lastSuccessfulRndNumber = Constants.NO_ROUND;

    private long _tries = 0;

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

    private States _currentState = States.EXIT;

    /**
     * In cases of ABORT, indicates the reason
     */
    private Event _event;

    private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

    private List<Proposal> _queue = new LinkedList<Proposal>();

    public Leader(FailureDetector aDetector, Transport aTransport, AcceptorLearner anAcceptorLearner) {
        _detector = aDetector;
        _transport = aTransport;
        _al = anAcceptorLearner;
    }

    public void shutdown() {
    	synchronized(this) {
    		_watchdog.cancel();

    		if (_membership != null)
    			_membership.dispose();
    		
    		_currentState = States.ABORT;
    	}
    }

    private long calculateLeaderRefresh() {
        long myExpiry = _al.getLeaderLeaseDuration();
        return myExpiry - (myExpiry * 20 / 100);
    }

    private long calculateInteractionTimeout() {
        return GRACE_PERIOD;
    }

    public long getCurrentRound() {
        synchronized(this) {
            return _rndNumber;
        }
    }

    public boolean isReady() {
        synchronized(this) {
            return ((_currentState.equals(States.EXIT)) || (_currentState.equals(States.ABORT)));
        }
    }

    private long nextSeqNum() {
    	if (_seqNum == Constants.UNKNOWN_SEQ)
    		return 0;
    	else
    		return _seqNum + 1;
    }
    
    private void advanceSeqNum() {
        // Possibility we're starting from scratch
        //
        if (_seqNum == Constants.UNKNOWN_SEQ)
            _seqNum = 0;
        else
            _seqNum = _seqNum + 1;
    }

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we
     * process the result of a Collect in the BEGIN state which means we expect Last or OldRound and in SUCCESS state
     * we expect ACCEPT or OLDROUND
     *
     * @todo Increment round number via heartbeats every so often - see note below about jittering collects.
     */
    private void process() {
        switch(_currentState) {
            case ABORT : {
            	assert (_queue.size() != 0);

                _logger.info(this + ": ABORT " + _event);

                _messages.clear();

                if (_membership != null)
                    _membership.dispose();

                cancelInteraction();
                
                /*
                 * We must fail all queued operations - use the same completion code...
                 */
                while (_queue.size() > 0) {
                    _al.signal(new Event(_event.getResult(), _event.getSeqNum(),
                            _queue.remove(0), _event.getLeader()));
                }

                return;
            }

            case EXIT : {
            	assert (_queue.size() != 0);

            	_logger.info(this + ": EXIT " + _event);

                _messages.clear();

                // Remove the just processed item
                //                
                _queue.remove(0);

                if (_membership != null)
                    _membership.dispose();

                if (_queue.size() > 0) {
                    _logger.info(this + ": processing op from queue: " + _queue.get(0));

                    _currentState = States.SUBMITTED;
                    process();

                } else {

                	// If we got here, we're leader, setup for a heartbeat if there's no other activity
                	//
                    _heartbeatAlarm = new TimerTask() {
                        public void run() {
                            _logger.info(this + ": sending heartbeat: " + System.currentTimeMillis());

                            submit(AcceptorLearner.HEARTBEAT);
                        }
                    };

                    _watchdog.schedule(_heartbeatAlarm, calculateLeaderRefresh());
                }

                return;
            }

            case SUBMITTED : {
            	assert (_queue.size() != 0);

                _tries = 0;
                _membership = _detector.getMembers(this);

                _logger.info(this + ": got membership: (" +
                        _membership.getSize() + ")");

                // Collect will decide if it can skip straight to a begin
                //
                _currentState = States.COLLECT;
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
            	assert (_queue.size() != 0);

            	// We've got activity, cancel heartbeat until we're done. Note this may be a heartbeat, that's okay.
            	//
            	if (_heartbeatAlarm != null) {
            		_heartbeatAlarm.cancel();
                    _watchdog.purge();
                }

            	Collect myLastCollect = _al.getLastCollect();

            	// Collect is INITIAL means no leader known so try to become leader
            	//
            	if (myLastCollect.isInitial()) {
            		_logger.info(this + ": collect is initial");

                    _rndNumber = 0;
            	} else {
            		InetSocketAddress myOtherLeader = myLastCollect.getNodeId();
            		boolean isUs = myOtherLeader.equals(_transport.getLocalAddress());

            		/*
            		 * If the leader is us we needn't update our round number and we can proceed, otherwise
            		 * we ascertain liveness of last known leader and proceed if it appears dead.
            		 *
            		 * This potential leader and its associated AL may be unaware of the active leader or indeed
            		 * it may appear dead. Under such circumstances this potential leader will attempt to
            		 * become anointed but could fail on an existing leader lease which would lead to a vote
            		 * timeout. The client would thus re-submit or similar rather than swap to the active leader node.
            		 * This is okay as eventually this potential leader will decide the active leader is alive and
            		 * immediately reject the client with an other leader message resulting in the necessary re-routing.
            		 *
            		 * The above behaviour has another benefit which is a client can connect to any member of the
            		 * paxos co-operative and be re-directed to the leader node. This means clients don't have to
            		 * perform any specialised behaviours for selecting a leader.
            		 */
            		if (! isUs) {

            			_logger.info(this + ": leader is not us");

            			if (_detector.isLive(myOtherLeader)) {
                            _logger.info(this + ": other leader is alive");

            				error(Event.Reason.OTHER_LEADER, myOtherLeader);
            				return;
            			} else {
            				_logger.info(this + ": other leader not alive");

                            /*
                             * If we're going for leadership, make sure we have a credible round number. If this
                             * round number isn't good enough, we'll find out via OldRound messages and update
                             * the round number accordingly.
                             */
                            if (_rndNumber <= myLastCollect.getRndNumber())
                                _rndNumber = myLastCollect.getRndNumber() + 1;
            			}
            		} else {
                        /*
                         * Optimisation - if the leader state machine successfully closed the last sequence number out,
                         * it can in state COLLECT issue a BEGIN and go to state success. The leader can deduce if it
                         * succeeded by recording the round number of it's last successful instance. It then compares
                         * that with the AL's view and if they match proceeds with the optimisation attempt. Note that
                         * the local AL may be out of date, that's okay as the Leader when it emits the BEGIN will be
                         * told OTHER_LEADER. It should then discard the last successful round number and when it
                         * retries, because it has no recollection of a previously successful round, it will issue a 
                         * COLLECT as usual. This is the multi-paxos optimisation.
                         */
                        if (_lastSuccessfulRndNumber == myLastCollect.getRndNumber()) {
                            _logger.info(this + ": applying multi-paxos");

                            _currentState = States.BEGIN;

                            process();

                            return;
                        }
                    }
            	}

            	_currentState = States.BEGIN;
                emit(new Collect(nextSeqNum(), _rndNumber, _transport.getLocalAddress()));

            	break;
            }

            case BEGIN : {
            	assert (_queue.size() != 0);

            	long myMaxProposal = -1;
            	Proposal myValue = null;

                for(PaxosMessage m : _messages) {
                    Last myLast = (Last) m;

                    if (!myLast.getConsolidatedValue().equals(LogStorage.NO_VALUE)) {
                        if (myLast.getRndNumber() > myMaxProposal) {
                            myValue = myLast.getConsolidatedValue();
                            myMaxProposal = myLast.getRndNumber();
                        }
                    }
                }

                /*
                 * If we have a value from a LAST message and it's not the same as the one we want to propose,
                 * we've hit an outstanding paxos instance and must now drive it to completion. Put it at the head of 
                 * the queue ready for the BEGIN. Note we must compare the consolidated value we want to propose
                 * as the one in the LAST message will be a consolidated value.
                 */
                if ((myValue != null) && (! myValue.equals(_queue.get(0))))
                	_queue.add(myValue);

                _currentState = States.SUCCESS;
                emit(new Begin(nextSeqNum(), _rndNumber, _queue.get(0), _transport.getLocalAddress()));

                break;
            }

            case SUCCESS : {
            	assert (_queue.size() != 0);

                if (_messages.size() >= _detector.getMajority()) {
                    // Send success
                    //
                    emit(new Success(nextSeqNum(), _rndNumber, _transport.getLocalAddress()));
                    cancelInteraction();
                    _lastSuccessfulRndNumber = _rndNumber;                    
                    successful(Event.Reason.DECISION);
                    advanceSeqNum();
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    emit(new Begin(nextSeqNum(), _rndNumber, _queue.get(0), _transport.getLocalAddress()));
                }

                break;
            }

            default : throw new RuntimeException("Invalid state: " + _currentState);
        }
    }

    private boolean canRetry() {
        return _currentState.equals(States.SUCCESS);
    }

    /**
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        OldRound myOldRound = (OldRound) aMessage;

        InetSocketAddress myCompetingNodeId = myOldRound.getLeaderNodeId();

        _lastSuccessfulRndNumber = Constants.NO_ROUND;

        /*
         * If we're getting an OldRound, the other leader's lease has expired. We may be about to become leader
         * but if we're proposing against an already settled sequence number we need to get up to date. If we were
         * about to become leader but our sequence number is out of date, the response we'll get from an AL is an
         * OldRound where the round number is less than ours but the sequence number is greater. Note that updating
         * our _seqNum will drive our own AL to recover missing state should that be necessary.
         */
        if (myOldRound.getLastRound() < _rndNumber) {
            if (myOldRound.getSeqNum() > nextSeqNum()) {
        	    _logger.info(this + ": This leader is out of date: " + nextSeqNum() + " < " + myOldRound.getSeqNum());

                _seqNum = myOldRound.getSeqNum();

                _currentState = States.COLLECT;
                process();
            }
        } else {
        	_logger.info(this + ": Another leader is active, backing down: " + myCompetingNodeId + " (" +
                myOldRound.getLastRound() + ", " + _rndNumber + ")");

            _rndNumber = myOldRound.getLastRound() + 1;
            error(Event.Reason.OTHER_LEADER, myCompetingNodeId);
        }
    }

    private void successful(int aReason) {
        _currentState = States.EXIT;
        _event = new Event(aReason, nextSeqNum(), _queue.get(0), _transport.getLocalAddress());

        process();
    }

    private void error(int aReason) {
    	error(aReason, _transport.getLocalAddress());
    }
    
    private void error(int aReason, InetSocketAddress aLeader) {
        _currentState = States.ABORT;
        _event = new Event(aReason, nextSeqNum(), _queue.get(0), aLeader);
        
        _logger.info("Leader encountered error: " + _event);

        process();
    }

    private void emit(PaxosMessage aMessage) {
        _messages.clear();

        if (startInteraction()) {
            _logger.info(this + ": sending: " + aMessage);

            _transport.send(aMessage, _transport.getBroadcastAddress());
        }
    }

    private boolean startInteraction() {
        assert _interactionAlarm == null;

        _interactionAlarm = new TimerTask() {
            public void run() {
                expired();
            }
        };

        _watchdog.schedule(_interactionAlarm, calculateInteractionTimeout());

        return _membership.startInteraction();
    }

    /**
     * @todo If we get ABORT, we could try a new round from scratch or make the client re-submit or .....
     */
    public void abort() {
        _logger.info(this + ": Membership requested abort");

        synchronized(this) {
            error(Event.Reason.BAD_MEMBERSHIP);
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
        _watchdog.purge();
        _interactionAlarm = null;
    }

    private void expired() {
        _logger.info(this + ": Watchdog requested abort: ");

        synchronized(this) {
            if (canRetry()) {
                ++_tries;

                if (_tries < MAX_TRIES) {
                	cancelInteraction();
                    process();
                    return;
                }
            }

            error(Event.Reason.VOTE_TIMEOUT);
        }
    }

    /**
     * Request a vote on a value.
     *
     * @param aValue is the value to attempt to agree upon
     */
    public void submit(Proposal aValue) {
        synchronized (this) {
        	_queue.add(aValue);

            if (! isReady()) {
                _logger.info(this + ": Queued operation (already active): " + aValue);
            } else {
                _logger.info(this + ": Queued operation (initialising leader)");

                _currentState = States.SUBMITTED;

                process();
            }
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
     * @param aMessage is a message from some acceptor/learner
     */
    public void messageReceived(PaxosMessage aMessage) {
        assert (aMessage.getClassification() != PaxosMessage.CLIENT): "Got a client message and shouldn't have done";

		_logger.info(this + " received message: " + aMessage);

        synchronized (this) {
            if ((aMessage.getSeqNum() == nextSeqNum()) &&
            		MessageValidator.getValidator(_currentState).acceptable(aMessage)) {
                if (MessageValidator.getValidator(_currentState).fail(aMessage)) {

                    // Can only be an oldRound right now...
                    //
                    oldRound(aMessage);
                } else {
                    _messages.add(aMessage);
                    _membership.receivedResponse(aMessage.getNodeId());
                }
            } else {
                _logger.warn(this + ": Unexpected message received: " + aMessage);
            }
        }
    }

    public String toString() {
        States myState;

        synchronized(this) {
            myState = _currentState;
        }

    	return "Leader: " + _transport.getLocalAddress() +
    		": (" + Long.toHexString(nextSeqNum()) + ", " + Long.toHexString(_rndNumber) + ")" + " in state: " + myState +
                " tries: " + _tries + "/" + MAX_TRIES;
    }

    private static abstract class MessageValidator {
        private static final MessageValidator _beginValidator =
                new MessageValidator(new Class[]{OldRound.class, Last.class}, new Class[]{OldRound.class}) {};

        private static final MessageValidator _successValidator =
                new MessageValidator(new Class[]{OldRound.class, Accept.class}, new Class[]{OldRound.class}){};

        private static final MessageValidator _nullValidator =
                new MessageValidator(new Class[]{}, new Class[]{}) {};

        static MessageValidator getValidator(States aLeaderState) {
            switch(aLeaderState) {
                case BEGIN : return _beginValidator;

                case SUCCESS : return _successValidator;

                default : {
                	_logger.warn("No validator for state: " + aLeaderState);
                	return _nullValidator;
                }
            }
        }

        private Class[] _acceptable;
        private Class[] _fail;

        private MessageValidator(Class[] acceptable, Class[] fail) {
            _acceptable = acceptable;
            _fail = fail;
        }

        boolean acceptable(PaxosMessage aMessage) {
            for (Class c: _acceptable) {
                if (aMessage.getClass().equals(c))
                    return true;
            }

            return false;
        }

        boolean fail(PaxosMessage aMessage) {
            for (Class c: _fail) {
                if (aMessage.getClass().equals(c))
                    return true;
            }

            return false;
        }
    }
}
