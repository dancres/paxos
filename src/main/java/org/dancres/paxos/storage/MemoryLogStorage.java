package org.dancres.paxos.storage;

import org.dancres.paxos.LogStorage;

import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemoryLogStorage implements LogStorage {
    private long _nextKey = 0;
    private final SortedMap<Long, byte[]> _log = new TreeMap<>();

    private boolean isClosed = false;
    private boolean isOpened = false;
    
	public void close() throws Exception {
		synchronized(this) {
			assert (isOpened);
			assert (!isClosed);

			isClosed = true;
		}
	}

	public void mark(long key, boolean force) throws Exception {
		synchronized(this) {
			Iterator<Long> myKeys = _log.keySet().iterator();
			while (myKeys.hasNext()) {
				Long myKey = myKeys.next();
				if (myKey < key)
					myKeys.remove();
			}
		}
	}

	public void open() throws Exception {
		synchronized(this) {
			assert (!isOpened);
			assert (!isClosed);
			
			isOpened = true;
		}
	}

	public long put(byte[] data, boolean sync) throws Exception {
		synchronized(this) {
			assert (isOpened);
			assert (!isClosed);
			
			long myKey = _nextKey;
			
			++_nextKey;			
			_log.put(myKey, data);
			
			return myKey;
		}
	}

	public byte[] get(long position) throws Exception {
		synchronized(this) {
			assert (isOpened);
			assert (!isClosed);
			
			return _log.get(position);
		}
	}
	
	public void replay(RecordListener listener, long mark) throws Exception {
		synchronized(this) {
			assert (isOpened);
			assert (!isClosed);

            for (Long myKey : _log.keySet()) {
                if (myKey >= mark) {
                    listener.onRecord(myKey, _log.get(myKey));
                }
            }
		}
	}
}
