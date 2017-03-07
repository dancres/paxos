package org.dancres.paxos.bus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessagesImpl<T extends Enum> implements Messages<T> {
    private final ConcurrentHashMap<String, Subscriber<T>> _subscribers = new ConcurrentHashMap<>();

    @Override
    public Subscription<T> subscribe(String aName, Subscriber<T> aSubs) {
        Object result = _subscribers.putIfAbsent(aName, aSubs);

        System.out.println("" + aSubs + ", " + result);

        if (result != null)
            throw new IllegalStateException("Already have a subscriber under the name: " + aName);

        return new Subscription<T>() {
            @Override
            public void send(T aType, Object aBody) {
                deliver(new MessageImpl<T>(aType, aBody, aName));
            }

            @Override
            public void send(T aType) {
                deliver(new MessageImpl<T>(aType, null, aName));
            }
        };
    }

    private void deliver(Message<T> aMsg) {
        for (Map.Entry<String, Subscriber<T>> aPair : _subscribers.entrySet()) {
            if (! aPair.getKey().equals(aMsg.getSource())) {
                System.out.println("Sending message to: " + aPair.getValue());
                aPair.getValue().msg(aMsg);
            }
        }
    }

    @Override
    public Map<String, Subscriber<T>> getSubscribers() {
        return _subscribers;
    }

    private class MessageImpl<T> implements Messages.Message<T> {
        private final T _type;
        private final Object _msg;
        private final String _source;

        MessageImpl(T aType, Object aMessage, String aSource) {
            _type = aType;
            _msg = aMessage;
            _source = aSource;
        }

        @Override
        public T getType() {
            return _type;
        }

        @Override
        public Object getMessage() {
            return _msg;
        }

        @Override
        public String getSource() {
            return _source;
        }
    }
}
