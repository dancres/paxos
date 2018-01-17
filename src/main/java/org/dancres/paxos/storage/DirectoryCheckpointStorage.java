package org.dancres.paxos.storage;

import org.dancres.paxos.CheckpointStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Comparator;

public class DirectoryCheckpointStorage implements CheckpointStorage {
    private static final Logger _logger = LoggerFactory.getLogger(DirectoryCheckpointStorage.class);
    private final File _dir;

    public DirectoryCheckpointStorage(File aDirectory) {
        _dir = aDirectory;
        _dir.mkdirs();
    }

    public ReadCheckpoint getLastCheckpoint() {
        final File[] myFiles = getFiles();
        
        return ((myFiles.length == 0) ? null : new ReadCheckpoint() {
            private InputStream _stream;

            public InputStream getStream() throws IOException {
                synchronized (this) {
                    if (_stream == null) {
                        _stream = new FileInputStream(myFiles[myFiles.length - 1]);                        
                    }
                    
                    return _stream;
                }
            }
        });
    }

    public WriteCheckpoint newCheckpoint() {
        return new WriteCheckpoint() {
            private File _temp;
            private FileOutputStream _stream;
            
            public void saved() {
                try {
                    _stream.getChannel().force(false);
                    _stream.close();
                } catch (Exception anE) {}

                if (! _temp.renameTo(new File(_dir, "ckpt" + Long.toString(System.currentTimeMillis()))))
                    _logger.warn("Couldn't rename checkpoint file" + _temp + ", " + _dir, new RuntimeException());
                
                File[] myFiles = getFiles();
                for (int i = 0; i <= myFiles.length - 2; i++) {
                    if (! myFiles[i].delete()) {
                        _logger.warn("Couldn't delete checkpoint file" + myFiles[i], new RuntimeException());
                    }
                }
            }

            public OutputStream getStream() throws IOException {
                synchronized (this) {
                    if (_temp == null) {
                        _temp = File.createTempFile("ckpt", null, _dir);
                        _stream = new FileOutputStream(_temp);
                    }
                    
                    return _stream;
                }
            }
        };
    }

    public int numFiles() {
        return getFiles().length;
    }

    private File[] getFiles() {
        File[] myFiles = _dir.listFiles((File file, String s) -> s.startsWith("ckpt"));

        assert (myFiles != null);

        Arrays.sort(myFiles, new Comparator<>() {
            public int compare(File file, File file1) {
                long fMod = file.lastModified();
                long f1Mod = file1.lastModified();
                
                if (fMod < f1Mod)
                    return -1;
                else if (fMod > f1Mod)
                    return 1;
                else
                    return 0;
            }

            public boolean equals(Object o) {
                return o.getClass().equals(this.getClass());
            }
        });

        return myFiles;
    }
    
    public static void main(String[] anArgs) throws Exception {
        DirectoryCheckpointStorage myStorage = new DirectoryCheckpointStorage(new File(anArgs[0]));
        
        File[] myFiles = myStorage.getFiles();

        for (File myFile : myFiles) System.out.println(myFile);
        
        WriteCheckpoint myCkpt = myStorage.newCheckpoint();
        ObjectOutputStream myOOS = new ObjectOutputStream(myCkpt.getStream());
        myOOS.writeObject(55);
        myOOS.close();
        myCkpt.saved();
        
        ReadCheckpoint myRestore = myStorage.getLastCheckpoint();
        ObjectInputStream myOIS = new ObjectInputStream(myRestore.getStream());
        System.out.println(myOIS.readObject());
        myOIS.close();
    }
}
