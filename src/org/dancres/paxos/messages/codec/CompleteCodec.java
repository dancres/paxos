package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.test.utils.Complete;

public class CompleteCodec implements Codec {

	public Object decode(ByteBuffer aBuffer) {
		aBuffer.getInt();
		
		return new Complete(aBuffer.getLong());
	}

	public ByteBuffer encode(Object anObject) {
		Complete myComp = (Complete) anObject;
		
		ByteBuffer myBuffer = ByteBuffer.allocate(4 + 8);
		myBuffer.putInt(Operations.COMPLETE);
		myBuffer.putLong(myComp.getSeqNum());
		myBuffer.flip();
		return myBuffer;
	}
}
