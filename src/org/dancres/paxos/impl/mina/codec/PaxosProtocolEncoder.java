package org.dancres.paxos.impl.mina.codec;

import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.common.IoSession;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.impl.faildet.Heartbeat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PaxosProtocolEncoder extends ProtocolEncoderAdapter {
    private Logger _logger;

    PaxosProtocolEncoder() {
        _logger = LoggerFactory.getLogger(getClass());
    }

    public void encode(IoSession aSession, Object anObject,
                       ProtocolEncoderOutput aProtocolEncoderOutput) throws Exception {
        PaxosMessage myMsg = (PaxosMessage) anObject;

        if (myMsg.getType() != Heartbeat.TYPE)
            _logger.info("Encoding: " + myMsg);

        int myType = myMsg.getType();
        Codec myCodec = Codecs.CODECS[myType];
        aProtocolEncoderOutput.write(myCodec.encode(myMsg));
    }
}
