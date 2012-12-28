package org.dancres.paxos.test.junit;

import org.dancres.paxos.CheckpointStorage;
import org.dancres.paxos.CheckpointStorage.*;
import org.dancres.paxos.storage.DirectoryCheckpointStorage;
import org.dancres.paxos.test.utils.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class CheckpointStorageTest {
    private static final String _checkpointDir = "ckptdir";
    
    @Before public void init() throws Exception {
        FileSystem.deleteDirectory(new File(_checkpointDir));
    }
    
    @Test public void test() throws Exception {
        CheckpointStorage myStorage = new DirectoryCheckpointStorage(new File(_checkpointDir));

        int myTotalFiles = myStorage.numFiles();
        Assert.assertTrue(myTotalFiles == 0);

        WriteCheckpoint myCkpt = myStorage.newCheckpoint();
        ObjectOutputStream myOOS = new ObjectOutputStream(myCkpt.getStream());
        myOOS.writeObject(new Integer(55));
        myOOS.close();
        myCkpt.saved();

        ReadCheckpoint myRestore = myStorage.getLastCheckpoint();
        ObjectInputStream myOIS = new ObjectInputStream(myRestore.getStream());
        Assert.assertTrue(new Integer(55).equals(myOIS.readObject()));
        myOIS.close();
    }
}
