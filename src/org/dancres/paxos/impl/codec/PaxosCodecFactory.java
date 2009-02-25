package org.dancres.paxos.impl.codec;

import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolEncoder;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.common.IoSession;

public class PaxosCodecFactory implements ProtocolCodecFactory {
    private PaxosProtocolDecoder _decoder = new PaxosProtocolDecoder();
    private PaxosProtocolEncoder _encoder = new PaxosProtocolEncoder();

    public ProtocolEncoder getEncoder(IoSession ioSession) throws Exception {
        return _encoder;
    }

    public ProtocolDecoder getDecoder(IoSession ioSession) throws Exception {
        return _decoder;
    }
}
