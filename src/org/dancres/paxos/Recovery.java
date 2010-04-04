package org.dancres.paxos;

import java.util.ArrayList;
import java.util.List;

import org.dancres.paxos.messages.Need;
import org.dancres.paxos.messages.PaxosMessage;

/*
 * If leader sends a request for a seqnum <= low_watermark, tell it ("out of date", low_watermark). Leader can now
 * tell it's AL to run recovery abort it's request. If request is for low_watermark + 1, process it otherwise own AL
 * needs recovery. Thus a collect for an old round is rejected and the log is guaranteed to be ordered correctly.
 *  
 * In supplying AL, if the first record in the log does not have seqNum <= min_seqnum in recovery window, then stop.
 * Also stop if low watermark is not >= max_seqnum in recovery window.
 */
public class Recovery {
	private static final short GATHER = 1;
	
	private AcceptorLearner _al;
	private Transport _transport;
	private FailureDetector _fd;
	private short _state = GATHER;
	
	private Interaction _interaction;
	
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


	private RecoveryWindow _recoveryWindow;
	
	/**
	 * If the sequence number we're seeing is for a sequence number > lwm + 1, we've missed some packets.
	 * Recovery range r is lwm < r <=x (where x = currentMessage.seqNum) 
	 * so that the round we're seeing right now is complete and we need save packets only after that point.
	 */	
	Recovery(long aMin, long aMax, FailureDetector aDetector, Transport aTransport, AcceptorLearner anAL) {
		_recoveryWindow = new RecoveryWindow(aMin, aMax);
		_fd = aDetector;
		_transport = aTransport;
		_interaction = new NeedImpl();
		_al = anAL;
	}
	
	/**
	 * @todo Implement recovery processing.
	 * 
	 * @param aMessage
	 */
	void messageReceived(PaxosMessage aMessage) {
		if (aMessage.getClassification() == PaxosMessage.LEADER) {
			/* 
			 * Standard protocol messages are discarded if they're below the recovery window.
			 * If they're in the window and for current_lwm + 1, we apply them and if they're greater
			 * than the window we store them. This allows us to easily stream packets from another AL without
			 * having to wrap them in some other message.
			 */
		} else {		
			// Packet is part of the recovery initiation protocol
			//
			_interaction.messageReceived(aMessage);
		}
	}
	
	class NeedImpl implements MembershipListener, Interaction {
		private List _messages = new ArrayList();
		private Membership _membership;
		
		NeedImpl() {
			emitNeed();
		}
		
		void emitNeed() {
			synchronized(_messages) {
				_messages.clear();
				_membership = _fd.getMembers(this);
				if (_membership.startInteraction())
					_transport.send(new Need(_recoveryWindow.getMinSeqNum(), _recoveryWindow.getMaxSeqNum(),
						_transport.getLocalNodeId().asLong()), NodeId.BROADCAST);
			}
		}
		
		public void messageReceived(PaxosMessage aMessage) {
			synchronized(_messages) {
				_messages.add(aMessage);
				_membership.receivedResponse(NodeId.from(aMessage.getNodeId()));
			}
		}

		/**
		 * @todo Implement re-send etc
		 */
		public void abort() {
		}

		/**
		 * @todo Dispatch next interaction
		 */
		public void allReceived() {
			_membership.dispose();
		}
	}
}
