package org.dancres.paxos.test.junit;

import java.io.File;

import org.dancres.paxos.test.utils.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.objectweb.howl.log.Configuration;
import org.objectweb.howl.log.LogException;
import org.objectweb.howl.log.LogRecord;
import org.objectweb.howl.log.LogRecordType;
import org.objectweb.howl.log.Logger;
import org.objectweb.howl.log.ReplayListener;

/**
 * Test to ensure Howl supports reads of an open and written-to log
 */
public class HowlTest {
	private static final String DIRECTORY = "howllogs";
	
	@Before public void init() throws Exception {
    	FileSystem.deleteDirectory(new File(DIRECTORY));
	}
	
	@Test public void test() throws Exception {
		Configuration myConfig = new Configuration();
		myConfig.setLogFileDir(DIRECTORY);
		
		Logger myLogger = new Logger(myConfig);
		myLogger.open();
		
		byte[] myArray;
		
		for (byte i = 0; i < 5; i++) {
			myArray = new byte[] {i};			
			myLogger.put(myArray, true);
		}
		
		ReplayListenerImpl myListener = new ReplayListenerImpl();
		myLogger.replay(myListener);
		
		myLogger.close();
		
		Assert.assertTrue(myListener.getError() == false);
		Assert.assertTrue(myListener.getRecordCount() == 6);
		Assert.assertTrue(myListener.gotEnd() == true);
	}
	
	private static class ReplayListenerImpl implements ReplayListener {
		private boolean _error = false;
		private int _recordCount = 0;
		private boolean _gotEnd = false;
		
		public LogRecord getLogRecord() {
			return new LogRecord(16);
		}

		public void onError(LogException anException) {
			_error = true;
			anException.printStackTrace(System.err);
		}

		public void onRecord(LogRecord aRecord) {
			++_recordCount;
			
			if (aRecord.type == LogRecordType.END_OF_LOG)
				_gotEnd = true;
		}		
		
		boolean getError() {
			return _error;
		}
		
		int getRecordCount() {
			return _recordCount;
		}
		
		boolean gotEnd() {
			return _gotEnd;
		}
	}
}
