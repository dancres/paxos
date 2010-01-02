package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.messages.PaxosMessage;
import org.dancres.paxos.impl.mina.io.ProposerHeader;
import org.dancres.paxos.impl.mina.io.ProposerPacket;

class ProposerReqCodec implements Codec {

	public Object decode(ByteBuffer aBuffer) {
		int myLength = aBuffer.getInt();
		
		// System.err.println("Length: " + myLength);
		
		int myOp = aBuffer.getInt();

		// System.err.println("Op: " + myOp);

		int myPort = aBuffer.getInt();
		
		// System.err.println("Port: " + myPort);
		
		// Read the nested op which is at the offset after port
        myOp = aBuffer.getInt(12);
        
		// System.err.println("Op: " + myOp);
        
        Codec myCodec = Codecs.CODECS[myOp];
        PaxosMessage myMsg = (PaxosMessage) myCodec.decode(aBuffer);
        
        return new ProposerHeader(myMsg, myPort);
	}

	public ByteBuffer encode(Object anObject) {
		ProposerPacket myMsg = (ProposerPacket) anObject;
		PaxosMessage myOperation = myMsg.getOperation();
		
		ByteBuffer myBuffer = encode(myOperation);
		ByteBuffer myPacket = ByteBuffer.allocate(4 + 4 + 4 + myBuffer.capacity());
		
		myPacket.putInt(4 + 4 + myBuffer.capacity());
		myPacket.putInt(Operations.PROPOSER_REQ);
		myPacket.putInt(myMsg.getPort());
		myPacket.put(myBuffer);
		
		myPacket.flip();
		
		return myPacket;
	}
	
	private ByteBuffer encode(PaxosMessage aMessage) {
        int myType = aMessage.getType();
        Codec myCodec = Codecs.CODECS[myType];
        
        return myCodec.encode(aMessage);		
	}
}
