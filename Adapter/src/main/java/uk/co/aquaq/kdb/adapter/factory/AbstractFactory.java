package uk.co.aquaq.kdb.adapter.factory;

import uk.co.aquaq.kdb.adapter.messages.MessageTypes;
import net.openhft.chronicle.wire.DocumentContext;

public interface AbstractFactory<A,B,C> {
    A readChronicleMessage(MessageTypes.AdapterMessageTypes adapterType, DocumentContext dc);
    int filterChronicleMessage(A msg, MessageTypes.AdapterMessageTypes adapterType, String filterField, String filterIn, String filterOut);
    B getKdbEnvelope(MessageTypes.AdapterMessageTypes adapterType, int maxEnvelopeSize);
    C mapChronicleToKdbMessage(MessageTypes.AdapterMessageTypes adapterType, A msg);
}
