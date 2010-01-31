package org.dancres.paxos.impl.util;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

import org.dancres.paxos.LogStorage;
import org.dancres.paxos.RecordListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.tools.javac.util.Log;

public class MemoryLogStorage implements LogStorage {
    private Logger _logger = LoggerFactory.getLogger(MemoryLogStorage.class);

    private long _nextKey = 0;    
    private SortedMap<Long, byte[]> _log = new TreeMap<Long, byte[]>();

    private boolean isClosed = false;
    private boolean isOpened = false;
    
	public void close() throws Exception {
		synchronized(this) {
			assert (isOpened == true);
			assert (isClosed == false);

			isClosed = true;
		}
	}

	public void mark(long key, boolean force) throws Exception {
		synchronized(this) {
			Iterator<Long> myKeys = _log.keySet().iterator();
			while (myKeys.hasNext()) {
				Long myKey = myKeys.next();
				if (myKey.longValue() < key)
					myKeys.remove();
			}
		}
	}

	public void open() throws Exception {
		synchronized(this) {
			assert (isOpened == false);
			assert (isClosed == false);
			
			isOpened = true;
		}
	}

	public long put(byte[] data, boolean sync) throws Exception {
		synchronized(this) {
			assert (isOpened == true);
			assert (isClosed == false);
			
			long myKey = _nextKey;
			
			++_nextKey;			
			_log.put(new Long(myKey), data);
			
			return myKey;
		}
	}

	public byte[] get(long position) throws Exception {
		synchronized(this) {
			assert (isOpened == true);
			assert (isClosed == false);
			
			return _log.get(new Long(position));
		}
	}
	
	public void replay(RecordListener listener, long mark) throws Exception {
		synchronized(this) {
			assert (isOpened == true);
			assert (isClosed == false);
			
			Iterator<Long> myKeys = _log.keySet().iterator();
			while (myKeys.hasNext()) {
				Long myKey = myKeys.next();
				
				if (myKey.longValue() >= mark) {
					listener.onRecord(myKey.longValue(), _log.get(myKey));
				}
			}
		}
	}
}
