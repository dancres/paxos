package org.dancres.paxos.messages.codec;

import java.nio.ByteBuffer;

import org.dancres.paxos.messages.Need;
import org.dancres.paxos.messages.Operations;

public class NeedCodec implements Codec {

	public ByteBuffer encode(Object anObject) {
		Need myNeed = (Need) anObject;
		
		ByteBuffer myBuffer;
		
		myBuffer = ByteBuffer.allocate(4 + 4 + 8 + 8 + 8);
		myBuffer.putInt(4 + 8 + 8 + 8);
		myBuffer.putInt(Operations.NEED);
		myBuffer.putLong(myNeed.getMinSeq());
		myBuffer.putLong(myNeed.getMaxSeq());
		myBuffer.putLong(myNeed.getNodeId());
		
		myBuffer.flip();
		return myBuffer;
	}

	public Object decode(ByteBuffer aBuffer) {
		// Discard length
		aBuffer.getInt();
		
		// Discard type
		aBuffer.getInt();
		
		long myMin = aBuffer.getLong();
		long myMax = aBuffer.getLong();
		long myNodeId = aBuffer.getLong();
		
		return new Need(myMin, myMax, myNodeId);
	}
}
