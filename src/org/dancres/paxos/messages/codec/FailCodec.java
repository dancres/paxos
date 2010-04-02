package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Operations;
import org.dancres.paxos.test.utils.Fail;

public class FailCodec implements Codec {

	public Object decode(ByteBuffer aBuffer) {
		aBuffer.getInt();
		
		return new Fail(aBuffer.getLong(), aBuffer.getInt());
	}

	public ByteBuffer encode(Object anObject) {
		Fail myFail = (Fail) anObject;
		
		ByteBuffer myBuffer = ByteBuffer.allocate(4 + 8 + 4);
		myBuffer.putInt(Operations.FAIL);
		myBuffer.putLong(myFail.getSeqNum());
		myBuffer.putInt(myFail.getReason());		
		myBuffer.flip();
		return myBuffer;
	}
}
