package org.dancres.paxos.impl;

import org.dancres.paxos.LogStorage;
import org.dancres.paxos.messages.PaxosMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Produces a set of <code>PaxosMessage</code> instances from <code>LogStorage</code> within a certain sequence
 * number range.
 */
class LogRangeProducer implements LogStorage.RecordListener, Producer {
	private static final Logger _logger = LoggerFactory.getLogger(LogRangeProducer.class);

	private final long _lowerBoundSeq;
    private final long _maximumSeq;
    private final Consumer _consumer;
    private final LogStorage _storage;
    private final Transport.PacketPickler _pickler;

    /**
     * @param aLowerBoundSeq the sequence number above (>) which to stream records.
     * @param aMaximumSeq the last sequence number in the range (<=) to stream.
     * @param aConsumer
     * @param aStorage
     */
    LogRangeProducer(long aLowerBoundSeq, long aMaximumSeq, Consumer aConsumer, LogStorage aStorage, Transport.PacketPickler aPickler) {
        _lowerBoundSeq = aLowerBoundSeq;
        _maximumSeq = aMaximumSeq;
        _consumer = aConsumer;
        _storage = aStorage;
        _pickler = aPickler;
    }

    public void produce(long aLogOffset) throws Exception {
        _storage.replay(this, 0);
    }

    public void onRecord(long anOffset, byte[] aRecord) {
        Transport.Packet myPacket = _pickler.unpickle(aRecord);
        PaxosMessage myMessage = myPacket.getMessage();

        // Only send messages in the specified window
        //
        if ((myMessage.getSeqNum() > _lowerBoundSeq)
                && (myMessage.getSeqNum() <= _maximumSeq)) {
            _logger.trace("Producing: " + myMessage);
            _consumer.process(myPacket, anOffset);
        } else {
            _logger.trace("Not producing: " + myMessage);
        }
    }
}
