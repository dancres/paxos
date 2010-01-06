package org.dancres.paxos;

import java.nio.ByteBuffer;

public class ConsolidatedValue {
	private byte[] _value;
	private byte[] _handback;
	
	public ConsolidatedValue(byte[] aValue, byte[] aHandback) {
		_value = aValue;
		_handback = aHandback;
	}
	
	public ConsolidatedValue(byte[] aMarshalled) {
        ByteBuffer myBuffer = ByteBuffer.wrap(aMarshalled);
        int myValueSize = myBuffer.getInt();

        _value = new byte[myValueSize];
        _handback = new byte[aMarshalled.length - 4 - myValueSize];
        myBuffer.get(_value);
        myBuffer.get(_handback);    	
	}
	
	public byte[] getValue() {
		return _value;
	}
	
	public byte[] getHandback() {
		return _handback;
	}
	
	public byte[] marshall() {
        ByteBuffer myBuffer = ByteBuffer.allocate(4 + _handback.length + _value.length);
        myBuffer.putInt(_value.length);
        myBuffer.put(_value);
        myBuffer.put(_handback);

        return myBuffer.array();		
	}
	
	public boolean equals(Object anObject) {
		if (anObject instanceof ConsolidatedValue) {
			ConsolidatedValue myOther = (ConsolidatedValue) anObject;
			
			if (compare(myOther._handback, _handback))
				return compare(myOther._value, _value);
		}
		
		return false;
	}
	
	private boolean compare(byte[] aFirst, byte[] aSecond) {
		if (aFirst.length != aSecond.length)
			return false;
		
		for (int i = 0; i < aFirst.length; i++) {
			if (aFirst[i] != aSecond[i])
				return false;
		}
		
		return true;
	}
}
