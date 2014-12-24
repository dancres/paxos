package org.dancres.paxos;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class Proposal {
    public static final Proposal NO_VALUE =
        	new Proposal("org.dancres.paxos.NoValue", new byte[0]);

	private final Map<String, byte[]> _values = new HashMap<>();
	
	public Proposal() {
	}
	
	public Proposal(String aKey, byte[] aValue) {
		_values.put(aKey, aValue);
	}
	
	public Proposal(byte[] aMarshalled) {
        ByteBuffer myBuffer = ByteBuffer.wrap(aMarshalled);
        int myNumValues = myBuffer.getInt();

        for (int i = 0; i < myNumValues; i++) {
        	int myKeySize = myBuffer.getInt();        	
        	byte[] myKey = new byte[myKeySize];
        	
        	myBuffer.get(myKey);
        	
        	int myValSize = myBuffer.getInt();        	
        	byte[] myVal = new byte[myValSize];
        	
        	myBuffer.get(myVal);
        	
        	_values.put(new String(myKey), myVal);
        }
	}

	public Proposal put(String aKey, byte[] aValue) {
		_values.put(aKey, aValue);
		return this;
	}
	
	public byte[] get(String aKey) {
		return _values.get(aKey);
	}
	
	public byte[] marshall() {
		int myBase = 0;
		
		for (Map.Entry<String, byte[]>kv : _values.entrySet()) {
			myBase += kv.getKey().getBytes().length;
			myBase += kv.getValue().length;
			
			// Include the per entry ints to represent length of key and value
			myBase += 8;
		}
		
        ByteBuffer myBuffer = ByteBuffer.allocate(4 + myBase);
        myBuffer.putInt(_values.size());
        
		for (Map.Entry<String, byte[]>kv : _values.entrySet()) {
			byte[] myKeyBytes = kv.getKey().getBytes();
			
			myBuffer.putInt(myKeyBytes.length);
			myBuffer.put(myKeyBytes);
			
			myBuffer.putInt(kv.getValue().length);
			myBuffer.put(kv.getValue());			
		}

        return myBuffer.array();		
	}
	
	public boolean equals(Object anObject) {
		if (anObject instanceof Proposal) {
			Proposal myOther = (Proposal) anObject;
	
			if (myOther.getSize() == getSize()) {
				for (Map.Entry<String, byte[]>kv : _values.entrySet()) {
					byte[] myOtherVal = myOther.get(kv.getKey());
					
					if ((myOtherVal == null) || (! compare(myOtherVal, kv.getValue())))
						return false;
				}
				
				return true;
			}
		}
		
		return false;
	}
	
	int getSize() {
		return _values.size();
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
	
	public String toString() {
		StringBuffer mySummary = new StringBuffer("Proposal (");
		
		for (String k: _values.keySet()) {
			mySummary.append(" ").append(k);
		}
		
		return mySummary.append(" )").toString();
	}
}
