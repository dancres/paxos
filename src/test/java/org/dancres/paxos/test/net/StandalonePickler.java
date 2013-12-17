package org.dancres.paxos.test.net;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.dancres.paxos.impl.Transport;
import org.dancres.paxos.messages.codec.Codecs;
import org.dancres.paxos.messages.PaxosMessage;

public class StandalonePickler implements Transport.PacketPickler {
    private InetSocketAddress _source;

    public StandalonePickler(InetSocketAddress aSource) {
        _source = aSource;
    }

    public Transport.Packet newPacket(PaxosMessage aMessage) {
        return new PacketImpl(aMessage, _source);
    }

    public byte[] pickle(Transport.Packet aPacket) {
        byte[] myBytes = Codecs.encode(aPacket.getMessage());
        ByteBuffer myBuffer = ByteBuffer.allocate(8 + 4 + myBytes.length);
        
        myBuffer.putLong(Codecs.flatten(aPacket.getSource()));
        myBuffer.putInt(myBytes.length);
        myBuffer.put(myBytes);
        myBuffer.flip();
        
        return myBuffer.array();
    }

    public Transport.Packet unpickle(byte[] aBytes) {
        ByteBuffer myBuffer = ByteBuffer.wrap(aBytes);
        
        InetSocketAddress mySource = Codecs.expand(myBuffer.getLong());
        int myLength = myBuffer.getInt();
        byte[] myPaxosBytes = new byte[myLength];
        myBuffer.get(myPaxosBytes);
        
        PaxosMessage myMessage = Codecs.decode(myPaxosBytes);
        
        return new PacketImpl(myMessage, mySource);         
    }

    class PacketImpl implements Transport.Packet {
        private PaxosMessage _msg;
        private InetSocketAddress _source;
        
        PacketImpl(PaxosMessage aMsg, InetSocketAddress aSource) {
            _msg = aMsg;
            _source = aSource;
        }
        
        public InetSocketAddress getSource() {
            return _source;
        }

        public PaxosMessage getMessage() {
            return _msg;
        }

        public String toString() {
            return "PK [ " + _source + " ] " + _msg;
        }
    }
}

