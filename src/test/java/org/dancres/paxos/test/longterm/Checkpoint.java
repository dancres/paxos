package org.dancres.paxos.test.longterm;

import org.dancres.paxos.CheckpointStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;

class Checkpoint {
    private static final Logger _logger = LoggerFactory.getLogger(Main.class);
    
    static void these(Deque<NodeAdmin> aNodes) {
        for (NodeAdmin myNA : aNodes) {
            try {
                myNA.checkpoint();
            } catch (Throwable aT) {
                // We can get one of these if the AL is currently OUT_OF_DATE, ignore it
                //
                _logger.warn("Exception at checkpoint", aT);
            }
        }

        // Bring any out of date nodes back up
        //
        for (NodeAdmin myNA : aNodes) {
            if (myNA.isOutOfDate())
                makeCurrent(myNA, aNodes);
        }
    }

    /**
     * Bring the specified NodeAdmin up to date via another checkpoint if there is one available
     *
     * @param anAdmin
     * @return <code>true</code> if the NodeAdmin was updated
     */
    static boolean makeCurrent(NodeAdmin anAdmin, Deque<NodeAdmin> aNodes) {
        for (NodeAdmin myNA : aNodes) {
            if ((! myNA.isOutOfDate()) && (myNA.lastCheckpointTime() > anAdmin.lastCheckpointTime())) {

                try {
                    CheckpointStorage.ReadCheckpoint myCkpt = myNA.getLastCheckpoint();

                    try {
                        if (anAdmin.bringUpToDate(myCkpt)) {
                            _logger.info("AL is now back up to date");

                            return true;
                        }
                    } catch (Exception anE) {
                        _logger.warn("Exception at bring up to date", anE);
                    }
                } catch (Exception anE) {
                    _logger.warn("Exception reading back checkpoint handle", anE);
                }
            }
        }

        _logger.info("Not able to update: " + anAdmin);

        return false;
    }

}
