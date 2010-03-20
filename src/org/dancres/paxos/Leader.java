package org.dancres.paxos;

import org.dancres.paxos.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
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

    private static final Map<Integer, Class[]> _acceptableResponses = new HashMap<Integer, Class[]>();
    
    static {
    	_acceptableResponses.put(new Integer(COMMITTED), new Class[] {Ack.class});
    	_acceptableResponses.put(new Integer(SUCCESS), new Class[] {OldRound.class, Accept.class});
    	_acceptableResponses.put(new Integer(BEGIN), new Class[] {OldRound.class, Last.class});
    }
    
    private final Timer _watchdog = new Timer("Leader timers");
    private final FailureDetector _detector;
    private final Transport _transport;
    private final AcceptorLearner _al;

    private long _seqNum = LogStorage.NO_SEQ;

    /**
     * Tracks the round number this leader used last. This cannot be stored in the acceptor/learner's version without 
     * risking breaking the collect protocol (the al could suddenly believe it's seen a collect it hasn't and become
     * noisy when it should be silent). This field can be updated as the result of OldRound messages.
     */
    private long _rndNumber = 0;

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
    private Event _event;

    private List<PaxosMessage> _messages = new ArrayList<PaxosMessage>();

    private List<Post> _queue = new LinkedList<Post>();

    public Leader(FailureDetector aDetector, Transport aTransport, AcceptorLearner anAcceptorLearner) {
        _detector = aDetector;
        _transport = aTransport;
        _al = anAcceptorLearner;
    }

    private long calculateLeaderRefresh() {
        long myExpiry = _al.getLeaderLeaseDuration();
        return myExpiry - (myExpiry * 20 / 100);
    }

    private long calculateInteractionTimeout() {
        return _detector.getUnresponsivenessThreshold() + FAILURE_DETECTOR_GRACE_PERIOD;    	
    }
    public long getCurrentRound() {
        synchronized(this) {
            return _rndNumber;
        }
    }

    private long getRndNumber() {
        return _rndNumber;
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

    public boolean isReady() {
        synchronized(this) {
            return (_stage == EXIT) || (_stage == ABORT);
        }
    }

    /**
     * Do actions for the state we are now in.  Essentially, we're always one state ahead of the participants thus we
     * process the result of a Collect in the BEGIN state which means we expect Last or OldRound and in SUCCESS state
     * we expect ACCEPT or OLDROUND
     *
     * @todo Increment round number via heartbeats every so often - see note below about jittering collects.
     */
    private void process() {
        switch(_stage) {
            case ABORT : {
            	assert (_queue.size() != 0);
            	
                _logger.info(this + ": ABORT " + _event, new RuntimeException());

                _messages.clear();
                
                // Remove the just processed item
                //
                _queue.remove(0);
                
                if (_membership != null)
                    _membership.dispose();

                _al.signal(_event);

                /*
                 * If there are any queued operations we must fail those also - use the same completion code...
                 */
                while (_queue.size() > 0) {
                    Post myOp = (Post) _queue.remove(0);
                    _al.signal(new Event(_event.getResult(), _event.getSeqNum(),
                            myOp.getConsolidatedValue()));
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

                    _stage = SUBMITTED;
                    process();

                } else {
                	
                	// If we got here, we're leader, setup for a heartbeat if there's no other activity
                	//
                    _heartbeatAlarm = new HeartbeatTask();
                    _watchdog.schedule(_heartbeatAlarm, calculateLeaderRefresh());
                }

                return;
            }

            case SUBMITTED : {
            	assert (_queue.size() != 0);
            	
                _membership = _detector.getMembers(this);

                _logger.info(this + ": got membership: (" +
                        _membership.getSize() + ")");

                // Collect will decide if it can skip straight to a begin
                //
                _stage = COLLECT;
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
            	if (_heartbeatAlarm != null)
            		_heartbeatAlarm.cancel();
            	
            	/*
            	 * If the Acceptor/Learner thinks there's a valid leader and the failure detector confirms
            	 * liveness, reject the request. Note that leader may be partitioned from the network with some 
            	 * collection of clients who may then try and use it as leader. The local AcceptorLearner will 
            	 * take the collect but there will be no majority. Clients might continue to hit the leader and at some
            	 * point it will become connected to the network.
            	 * 
            	 * During all this time it's updating it's round number and ultimately it would overpower the other
            	 * leader when it reconnects because acceptor learners only extend their lease when they receive a
            	 * begin from a leader (which means it succeeded with a collect). This can result in collect jitters
            	 * where the partitioned master returns with a higher round number and wins when it submits a collect.
            	 * We don't want this to happen too often. 
            	 * 
            	 * The above jittering is addressed by having clients process the vote_timeout message they will receive
            	 * from the partitioned leader and attempt to locate the last known leader for some period of time 
            	 * before returning to this one for another attempt. This slows the rate at which the partitioned leader
            	 * increments it's round number. In addition we have the active leader increment it's own round number
            	 * after some number of heartbeats to further reduce the likelihood of this jittering.
            	 */
            	Collect myLastCollect = _al.getLastCollect();

            	// Collect is INITIAL means no leader known so try to become leader
            	//
            	if (myLastCollect.isInitial()) {
                	// Best guess for a round number is the acceptor/learner
                	//
            		_logger.info(this + ": collect is initial");
            		
                	updateRndNumber(myLastCollect.getRndNumber() + 1);            		
            	} else { 
            		NodeId myOtherLeader = NodeId.from(myLastCollect.getNodeId());
            		boolean isUs = myOtherLeader.equals(_transport.getLocalNodeId());
            		
            		/*
            		 *  If the leader is us, use our existing round number, otherwise ascertain liveness and if we are
            		 *  going to proceed invent a new round number.
            		 */
            		if (! isUs) {
            			
            			_logger.info(this + ": leader is not us");
            			
            			if (_detector.isLive(myOtherLeader)) {
            				error(Event.Reason.OTHER_LEADER, myOtherLeader);
            				return;
            			} else {
                        	// Best guess for a round number is the acceptor/learner
                        	//
            				
            				_logger.info(this + ": other leader not alive");
            				
                        	updateRndNumber(myLastCollect.getRndNumber() + 1);            		            				
            			}
            		}
            	}

            	// Best guess for starting sequence number is the acceptor/learner
            	//
            	_seqNum = _al.getLowWatermark().getSeqNum();

            	// Possibility we're starting from scratch
            	//
            	if (_seqNum == LogStorage.NO_SEQ)
            		_seqNum = 0;
            	else
            		_seqNum = _seqNum + 1;

            	_stage = BEGIN;
            	emitCollect();
            	
            	break;
            }

            case BEGIN : {
            	assert (_queue.size() != 0);
            	
            	long myMaxProposal = -1;
            	ConsolidatedValue myValue = null;
            	
                Iterator<PaxosMessage> myMessages = _messages.iterator();

                while (myMessages.hasNext()) {
                    PaxosMessage myMessage = myMessages.next();

                    if (myMessage.getType() == Operations.OLDROUND) {
                        /*
                         * An OldRound here indicates some other leader is present, give up.
                         */
                        oldRound(myMessage);
                        return;
                    } else {
                        Last myLast = (Last) myMessage;
                        
                        if (! myLast.getConsolidatedValue().equals(LogStorage.NO_VALUE)) {
                        	if (myLast.getRndNumber() > myMaxProposal)
                        		myValue = myLast.getConsolidatedValue();
                        }
                    }
                }

                /*
                 * If we have a value from a LAST message and it's not the same as the one we want to propose, 
                 * we've hit an outstanding paxos instance and must now drive it to completion. Put it at the head of 
                 * the queue ready for the BEGIN. Note we must compare the consolidated value we want to propose
                 * as the one in the LAST message will be a consolidated value.
                 */
                if ((myValue != null) && (! myValue.equals(_queue.get(0).getConsolidatedValue())))
                	_queue.add(new Post(myValue, NodeId.MOST_SUBORDINATE.asLong()));

                _stage = SUCCESS;
                emitBegin();
                
                break;
            }

            case SUCCESS : {
            	assert (_queue.size() != 0);
            	
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
                    _stage = COMMITTED;
                    emitSuccess();
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    _stage = SUCCESS;
                    emitBegin();
                }

                break;
            }

            case COMMITTED : {
            	assert (_queue.size() != 0);
            	
                /*
                 * If ACK messages total more than majority we're happy otherwise try again.
                 */
                if (_messages.size() >= _membership.getMajority()) {
                    successful(Event.Reason.DECISION, null);
                } else {
                    // Need another try, didn't get enough accepts but didn't get leader conflict
                    //
                    _stage = COMMITTED;
                    emitSuccess();
                }

                break;
            }

            default : throw new RuntimeException("Invalid state: " + _stage);
        }
    }

    private class HeartbeatTask extends TimerTask {
        public void run() {
            _logger.info(this + ": sending heartbeat: " + System.currentTimeMillis());

            /*
             * Don't have to check the return value - if it's accepted we weren't active, otherwise we are and
             * no heartbeat is required. Note that a heartbeat could cause us to decide we're no longer leader
             * although that's unlikely if things are stable as no other node can become leader whilst we hold the
             * lease
             */
            submit(new Post(AcceptorLearner.HEARTBEAT, NodeId.MOST_SUBORDINATE.asLong()));
        }
    }

    /**
     * @param aMessage is an OldRound message received from some other node
     */
    private void oldRound(PaxosMessage aMessage) {
        OldRound myOldRound = (OldRound) aMessage;

        NodeId myCompetingNodeId = NodeId.from(myOldRound.getLeaderNodeId());

        //Some other node is active, we should abort.
        //
        _logger.info(this + ": Another leader is active, backing down: " + myCompetingNodeId);
        
        error(Event.Reason.OTHER_LEADER, myCompetingNodeId);
    }

    private void successful(int aReason, Object aContext) {
        _stage = EXIT;
        _event = new Event(aReason, _seqNum, _queue.get(0).getConsolidatedValue(), aContext);

        process();
    }

    private void error(int aReason, Object aContext) {
        _stage = ABORT;
        _event = new Event(aReason, _seqNum, _queue.get(0).getConsolidatedValue(), aContext);

        process();
    }

    private void emitCollect() {
        _messages.clear();

        PaxosMessage myMessage = new Collect(_seqNum, getRndNumber(), _transport.getLocalNodeId().asLong());

        if (!startInteraction())
        	return;

        _logger.info(this + ": sending collect");

        _transport.send(myMessage, NodeId.BROADCAST);
    }

    private void emitBegin() {
        _messages.clear();

        PaxosMessage myMessage = new Begin(_seqNum, getRndNumber(), _queue.get(0).getConsolidatedValue(),
        		_transport.getLocalNodeId().asLong());

        if (!startInteraction())
        	return;

        _logger.info(this + ": sending begin");

        _transport.send(myMessage, NodeId.BROADCAST);
    }

    private void emitSuccess() {
        _messages.clear();

        PaxosMessage myMessage = new Success(_seqNum, getRndNumber(),
        		_queue.get(0).getConsolidatedValue(), _transport.getLocalNodeId().asLong());

        if (!startInteraction())
        	return;

        _logger.info(this + ": sending success");

        _transport.send(myMessage, NodeId.BROADCAST);
    }

    private boolean startInteraction() {
        _interactionAlarm = new InteractionAlarm();
        _watchdog.schedule(_interactionAlarm, calculateInteractionTimeout());

        return _membership.startInteraction();
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
        _logger.info(this + ": Membership requested abort");

        _interactionAlarm.cancel();
        
        synchronized(this) {
            error(Event.Reason.BAD_MEMBERSHIP, null);
        }
    }

    private void expired() {
        _logger.info(this + ": Watchdog requested abort: ");

        synchronized(this) {
            // If there are enough messages we might get a majority continue
            //
            if (_messages.size() >= _membership.getMajority())
                process();
            else {
                error(Event.Reason.VOTE_TIMEOUT, null);
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
        	_queue.add(anOp);
        	
            if (! isReady()) {
                _logger.info(this + ": Queued operation (already active): " + anOp);
                return;
            }

            _logger.info(this + ": Queued operation (initialising leader)");

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
    public void messageReceived(PaxosMessage aMessage) {
    	if (aMessage.getClassification() == PaxosMessage.CLIENT) {
    		submit((Post) aMessage);
    		return;
    	}
    	
		_logger.info(this + " received message: " + aMessage);
        
        synchronized (this) {
            if ((aMessage.getSeqNum() == _seqNum) && acceptable(aMessage)) {
                _messages.add(aMessage);
                _membership.receivedResponse(NodeId.from(aMessage.getNodeId()));
            } else {
                _logger.warn(this + ": Unexpected message received: " + aMessage.getSeqNum());
            }
        }
    }
    
    private boolean acceptable(PaxosMessage aMessage) {
    	Class[] myAcceptableTypes = _acceptableResponses.get(new Integer(_stage));
    	
    	if (myAcceptableTypes == null)
    		throw new RuntimeException("Not got a set of expected types for this state :(");
    	
    	for (int i = 0; i < myAcceptableTypes.length; i++) {
    		if (aMessage.getClass().equals(myAcceptableTypes[i]))
    			return true;
    	}
    	
    	return false;
    }
    
    public String toString() {
    	return "Leader: " + _transport.getLocalNodeId() +
    		": (" + Long.toHexString(_seqNum) + ", " + Long.toHexString(_rndNumber) + ")";
    }
}
