package uk.co.aquaq.kdb.adapter.factory;

import uk.co.aquaq.kdb.adapter.envelopes.KdbEnvelope;
import uk.co.aquaq.kdb.adapter.messages.ChronicleMessage;
import uk.co.aquaq.kdb.adapter.messages.KdbMessage;
import uk.co.aquaq.kdb.adapter.messages.MessageTypes;
import net.openhft.chronicle.wire.DocumentContext;

public interface AbstractFactory<A extends ChronicleMessage,B extends KdbEnvelope,C extends KdbMessage> {
    A readChronicleMessage(MessageTypes.AdapterMessageTypes adapterType, DocumentContext dc);
    B getKdbEnvelope(MessageTypes.AdapterMessageTypes adapterType, int maxEnvelopeSize);
    C mapChronicleToKdbMessage(MessageTypes.AdapterMessageTypes adapterType, A msg);
}
