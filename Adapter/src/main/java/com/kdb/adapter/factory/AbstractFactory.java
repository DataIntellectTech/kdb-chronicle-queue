package com.kdb.adapter.factory;

import com.kdb.adapter.messages.ChronicleMessage;
import com.kdb.adapter.messages.MessageTypes;
import net.openhft.chronicle.wire.DocumentContext;

public interface AbstractFactory<A,B,C> {
    A readChronicleMessage(MessageTypes.AdapterMessageTypes adapterType, DocumentContext dc, String messageType);
    B getKdbEnvelope(MessageTypes.AdapterMessageTypes adapterType, int maxEnvelopeSize);
    C mapChronicleToKdbMessage(MessageTypes.AdapterMessageTypes adapterType, A msg);
}
