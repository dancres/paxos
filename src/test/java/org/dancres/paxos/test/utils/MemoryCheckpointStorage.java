package org.dancres.paxos.test.utils;

import org.dancres.paxos.CheckpointStorage;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class MemoryCheckpointStorage implements CheckpointStorage {
    private List<byte[]> _checkpoints = new LinkedList<byte[]>();

    public int numFiles() {
        synchronized(this) {
            return _checkpoints.size();
        }
    }

    public ReadCheckpoint getLastCheckpoint() {
        synchronized(this) {
            return ((_checkpoints.size() == 0) ? null : new ReadCheckpoint() {
                private InputStream _stream;

                public InputStream getStream() throws IOException {
                    synchronized (this) {
                        if (_stream == null) {
                            _stream = new ByteArrayInputStream(_checkpoints.get(0));
                        }

                        return _stream;
                    }
                }
            });

        }
    }

    public WriteCheckpoint newCheckpoint() {
        return new WriteCheckpoint() {
            private ByteArrayOutputStream _stream;

            public void saved() {
                synchronized(this) {
                    try {
                        _stream.close();
                    } catch (Exception anE) {
                    }

                    if (_checkpoints.size() != 0)
                        _checkpoints.remove(0);

                    _checkpoints.add(_stream.toByteArray());
                }
            }

            public OutputStream getStream() throws IOException {
                synchronized (this) {
                    if (_stream == null) {
                        _stream = new ByteArrayOutputStream();
                    }

                    return _stream;
                }
            }
        };
    }
}
