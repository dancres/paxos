package org.dancres.paxos.storage;

import org.dancres.paxos.LogStorage;

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryLogStorage implements LogStorage {
    private AtomicLong _nextKey = new AtomicLong(0);
    private final ConcurrentSkipListMap<Long, byte[]> _log = new ConcurrentSkipListMap<>();

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
        Iterator<Long> myKeys = _log.keySet().iterator();
        while (myKeys.hasNext()) {
            Long myKey = myKeys.next();
            if (myKey < key)
                myKeys.remove();
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
		}

        long myKey = _nextKey.getAndIncrement();

        _log.put(myKey, data);

        return myKey;
	}

	public byte[] get(long position) throws Exception {
		synchronized(this) {
			assert (isOpened);
			assert (!isClosed);
		}

        return _log.get(position);
	}

	public void replay(RecordListener listener, long mark) throws Exception {
		synchronized(this) {
			assert (isOpened);
			assert (!isClosed);
		}

        ConcurrentSkipListMap<Long, byte[]> myCopy = _log.clone();

		for (Long myKey : myCopy.keySet()) {
			if (myKey >= mark) {
				listener.onRecord(myKey, myCopy.get(myKey));
			}
		}
	}
}
