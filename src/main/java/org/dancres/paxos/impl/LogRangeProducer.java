package org.dancres.paxos.impl;

import org.dancres.paxos.LogStorage;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.messages.codec.Codecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces a set of <code>PaxosMessage</code> instances from <code>LogStorage</code> within a certain sequence
 * number range.
 */
class LogRangeProducer implements LogStorage.RecordListener, Producer {
	private static Logger _logger = LoggerFactory.getLogger(LogRangeProducer.class);

	private long _lowerBoundSeq;
    private long _maximumSeq;
    private Consumer _consumer;
    private LogStorage _storage;

    /**
     * @param aLowerBoundSeq the sequence number above (>) which to stream records.
     * @param aMaximumSeq the last sequence number in the range (<=) to stream.
     * @param aConsumer
     * @param aStorage
     */
    LogRangeProducer(long aLowerBoundSeq, long aMaximumSeq, Consumer aConsumer, LogStorage aStorage) {
        _lowerBoundSeq = aLowerBoundSeq;
        _maximumSeq = aMaximumSeq;
        _consumer = aConsumer;
        _storage = aStorage;
    }

    public void produce(long aLogOffset) throws Exception {
        _storage.replay(this, 0);
    }

    public void onRecord(long anOffset, byte[] aRecord) {
        PaxosMessage myMessage = Codecs.decode(aRecord);

        // Only send messages in the specified window
        //
        if ((myMessage.getSeqNum() > _lowerBoundSeq)
                && (myMessage.getSeqNum() <= _maximumSeq)) {
            _logger.debug("Producing: " + myMessage);
            _consumer.process(myMessage, anOffset);
        } else {
            _logger.debug("Not producing: " + myMessage);
        }
    }
}
