package org.dancres.paxos.messages.codec;

import org.dancres.paxos.messages.Need;
import org.dancres.paxos.messages.Operations;

import java.nio.ByteBuffer;

public class NeedCodec implements Codec {

	public ByteBuffer encode(Object anObject) {
		Need myNeed = (Need) anObject;
		
		ByteBuffer myBuffer;
		
		myBuffer = ByteBuffer.allocate(4 + 8 + 8);
		myBuffer.putInt(Operations.NEED);
		myBuffer.putLong(myNeed.getMinSeq());
		myBuffer.putLong(myNeed.getMaxSeq());
		
		myBuffer.flip();
		return myBuffer;
	}

	public Object decode(ByteBuffer aBuffer) {
		// Discard type
		aBuffer.getInt();
		
		long myMin = aBuffer.getLong();
		long myMax = aBuffer.getLong();
		
		return new Need(myMin, myMax);
	}
}
