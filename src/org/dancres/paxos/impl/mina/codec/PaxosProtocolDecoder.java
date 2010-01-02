package org.dancres.paxos.impl.mina.codec;

import org.apache.mina.filter.codec.ProtocolDecoderOutput;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.common.IoSession;
import org.apache.mina.common.IoBuffer;
import org.dancres.paxos.impl.faildet.Heartbeat;
import org.dancres.paxos.messages.codec.Codec;
import org.dancres.paxos.messages.codec.Codecs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PaxosProtocolDecoder extends CumulativeProtocolDecoder {
    private Logger _logger;

    PaxosProtocolDecoder() {
        _logger = LoggerFactory.getLogger(getClass());
    }

    protected boolean doDecode(IoSession aSession, IoBuffer aBuffer,
                               ProtocolDecoderOutput aProtocolDecoderOutput)
            throws Exception {

        if (!aBuffer.prefixedDataAvailable(4)) {
            _logger.warn("!!!!!!!!!!!!!! Faulty or fragmented packet !!!!!!!!!!!!!!");
            return false;
        }

        if (aBuffer.getInt(4) != Heartbeat.TYPE)
            _logger.info("Decoding: " + aBuffer.getInt(4));

        int myOp = aBuffer.getInt(4);
        Codec myCodec = Codecs.CODECS[myOp];
        aProtocolDecoderOutput.write(myCodec.decode(aBuffer.buf()));
        return true;
    }
}
