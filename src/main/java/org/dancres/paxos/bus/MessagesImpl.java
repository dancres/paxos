package org.dancres.paxos.bus;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MessagesImpl<T extends Enum> implements Messages<T> {
    private final ConcurrentHashMap<String, Subscriber<T>> _subscribers = new ConcurrentHashMap<>();
    private final AtomicLong _anonSubscript = new AtomicLong(0);

    @Override
    public Subscription<T> subscribe(String aName, Subscriber<T> aSubs) {
        Object result = _subscribers.putIfAbsent(aName, aSubs);

        if (result != null)
            throw new IllegalStateException("Already have a subscriber under the name: " + aName);

        return new Subscription<T>() {
            @Override
            public void send(T aType, Object aBody) {
                deliver(new MessageImpl<>(aType, aBody, aName));
            }

            @Override
            public void send(T aType) {
                deliver(new MessageImpl<>(aType, null, aName));
            }

            @Override
            public void unsubscribe() {
                _subscribers.remove(aName);
            }
        };
    }

    @Override
    public Subscription<T> anonSubscrbe(Subscriber<T> aSubs) {
        return subscribe("Anon:" + _anonSubscript.getAndIncrement(), aSubs);
    }

    private void deliver(Message<T> aMsg) {
        for (Map.Entry<String, Subscriber<T>> aPair : _subscribers.entrySet()) {
            if (! aPair.getKey().equals(aMsg.getSource()))
                aPair.getValue().msg(aMsg);
        }
    }

    @Override
    public Map<String, Subscriber<T>> getSubscribers() {
        return _subscribers;
    }

    private class MessageImpl<U> implements Messages.Message<U> {
        private final U _type;
        private final Object _msg;
        private final String _source;

        MessageImpl(U aType, Object aMessage, String aSource) {
            _type = aType;
            _msg = aMessage;
            _source = aSource;
        }

        @Override
        public U getType() {
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
