package org.dancres.paxos.storage;

import org.dancres.paxos.LogStorage;
import org.objectweb.howl.log.Configuration;
import org.objectweb.howl.log.LogException;
import org.objectweb.howl.log.LogRecord;
import org.objectweb.howl.log.LogRecordType;
import org.objectweb.howl.log.Logger;
import org.objectweb.howl.log.ReplayListener;

public class HowlLogger implements LogStorage {
	private static final int DEFAULT_RECORD_SIZE = 512;
	
	private Logger _logger;
	private final String _dir;
	
	public HowlLogger(String aDirectory) {
		_dir = aDirectory;
	}
	
	public void close() throws Exception {
		assert (_logger != null);
		
		_logger.close();
	}

	public byte[] get(long mark) throws Exception {
		LogRecord myRecord = new LogRecord(DEFAULT_RECORD_SIZE);
		
		return _logger.get(myRecord, mark).data;
	}

	public void mark(long key, boolean force) throws Exception {
		_logger.mark(key, force);
	}

	public void open() throws Exception {
		Configuration myConfig = new Configuration();
		myConfig.setLogFileDir(_dir);
		_logger = new Logger(myConfig);
		_logger.open();
	}

	public long put(byte[] data, boolean sync) throws Exception {
		return _logger.put(data, sync);
	}

	public void replay(RecordListener listener, long mark) throws Exception {
		_logger.replay(new ListenerAdapter(listener), mark);
	}
	
	private static class ListenerAdapter implements ReplayListener {
		private final RecordListener _listener;
		
		ListenerAdapter(RecordListener aListener) {
			_listener = aListener;
		}

		public LogRecord getLogRecord() {
			return new LogRecord(DEFAULT_RECORD_SIZE);
		}

		public void onError(LogException anError) {
			throw new RuntimeException("Logger failed: " + anError);
		}

		public void onRecord(LogRecord aRecord) {
			if (aRecord.type != LogRecordType.END_OF_LOG) {
				byte[][] myFields = aRecord.getFields();
				
				/*
				for (int i = 0; i < myFields.length; i++) {
					dump("Field: " + i, myFields[i]);
				}
				*/
				
				_listener.onRecord(aRecord.key, myFields[0]);
			}
		}
		
	    private void dump(String aMessage, byte[] aBuffer) {
	    	System.err.print(aMessage + " ");
	    	
	        for (int i = 0; i < aBuffer.length; i++) {
	            System.err.print(Integer.toHexString(aBuffer[i]) + " ");
	        }

	        System.err.println();
	    }			
	}
}
