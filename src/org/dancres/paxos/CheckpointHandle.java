package org.dancres.paxos;

import java.io.Serializable;

public abstract class CheckpointHandle implements Serializable {
    public static final CheckpointHandle NO_CHECKPOINT =
            new CheckpointHandle() {
                public void saved() {}};

    public abstract void saved() throws Exception;
}
