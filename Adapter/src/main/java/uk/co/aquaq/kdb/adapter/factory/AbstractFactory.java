package uk.co.aquaq.kdb.adapter.factory;

import uk.co.aquaq.kdb.adapter.messages.MessageTypes;
import net.openhft.chronicle.wire.DocumentContext;

public interface AbstractFactory<A,B,C> {
    A readChronicleMessage(MessageTypes.AdapterMessageTypes adapterType, DocumentContext dc);
    B getKdbEnvelope(MessageTypes.AdapterMessageTypes adapterType, int maxEnvelopeSize);
    C mapChronicleToKdbMessage(MessageTypes.AdapterMessageTypes adapterType, A msg);
}
