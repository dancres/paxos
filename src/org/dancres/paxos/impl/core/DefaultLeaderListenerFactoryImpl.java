package org.dancres.paxos.impl.core;

import org.dancres.paxos.impl.core.messages.Ack;

public class DefaultLeaderListenerFactoryImpl implements LeaderListenerFactory {
    private Channel _channel;

    public DefaultLeaderListenerFactoryImpl(Channel aChannel) {
        _channel = aChannel;
    }

    public LeaderListener newListener() {
        return new LeaderListenerImpl();
    }

    class LeaderListenerImpl implements LeaderListener {

        LeaderListenerImpl() {
        }

        /**
         * @todo Send a failure message to client on abort
         *
         * @param aLeader for which the state has changed
         */
        public void newState(Leader aLeader) {
            switch (aLeader.getState()) {
                case Leader.EXIT: {
                    _channel.write(new Ack(aLeader.getSeqNum()));

                    break;
                }

                case Leader.ABORT: {
                    break;
                }

                default: {
                    break;
                }
            }
        }
    }
}
