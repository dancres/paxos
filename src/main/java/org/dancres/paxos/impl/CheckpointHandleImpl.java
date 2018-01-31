package org.dancres.paxos.impl;

import org.dancres.paxos.CheckpointHandle;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

class CheckpointHandleImpl implements CheckpointHandle {
    private static final long serialVersionUID = -7904255942396151132L;

    private transient Watermark _lowWatermark;
    private transient Transport.Packet _lastCollect;
    private transient Transport.PacketPickler _pr;

    CheckpointHandleImpl(Watermark aLowWatermark, Transport.Packet aCollect, Transport.PacketPickler aPickler) {
        _lowWatermark = aLowWatermark;
        _lastCollect = aCollect;
        _pr = aPickler;
    }

    private void readObject(ObjectInputStream aStream) throws IOException, ClassNotFoundException {
        aStream.defaultReadObject();
        _lowWatermark = new Watermark(aStream.readLong(), aStream.readLong());
        _pr = (Transport.PacketPickler) aStream.readObject();

        byte[] myBytes = new byte[aStream.readInt()];
        aStream.readFully(myBytes);
        _lastCollect = _pr.unpickle(myBytes);
    }

    private void writeObject(ObjectOutputStream aStream) throws IOException {
        aStream.defaultWriteObject();
        aStream.writeLong(_lowWatermark.getSeqNum());
        aStream.writeLong(_lowWatermark.getLogOffset());
        aStream.writeObject(_pr);

        byte[] myCollect = _pr.pickle(_lastCollect);
        aStream.writeInt(myCollect.length);
        aStream.write(myCollect);
    }

    public boolean isNewerThan(CheckpointHandle aHandle) {
        if (aHandle.equals(CheckpointHandle.NO_CHECKPOINT))
            return true;
        else if (aHandle instanceof CheckpointHandleImpl) {
            CheckpointHandleImpl myOther = (CheckpointHandleImpl) aHandle;

            return (_lowWatermark.getSeqNum() > myOther._lowWatermark.getSeqNum());
        } else {
            throw new IllegalArgumentException("Where did you get this checkpoint from?");
        }
    }

    public long getTimestamp() {
        return _lowWatermark.getSeqNum();
    }

    Watermark getLowWatermark() {
        return _lowWatermark;
    }

    Transport.Packet getLastCollect() {
        return _lastCollect;
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof CheckpointHandleImpl) {
            CheckpointHandleImpl myOther = (CheckpointHandleImpl) anObject;

            return ((_lowWatermark.equals(myOther._lowWatermark)) &&
                    (_lastCollect.getSource().equals(myOther._lastCollect.getSource())) &&
                    (_lastCollect.getMessage().equals(myOther._lastCollect.getMessage())));
        }

        return false;
    }
}
