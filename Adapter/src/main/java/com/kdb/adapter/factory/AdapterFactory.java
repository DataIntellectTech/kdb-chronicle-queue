package com.kdb.adapter.factory;

import com.kdb.adapter.mapper.QuoteMapper;
import com.kdb.adapter.messages.*;
import net.openhft.chronicle.wire.DocumentContext;
import org.mapstruct.factory.Mappers;

public class AdapterFactory implements AbstractFactory<ChronicleMessage,KdbEnvelope, KdbMessage> {

    private final QuoteMapper quoteMapper = Mappers.getMapper(QuoteMapper.class);

    @Override
    public ChronicleMessage readChronicleMessage(MessageTypes.AdapterMessageTypes adapterType, DocumentContext dc, String messageType){
        if (MessageTypes.AdapterMessageTypes.QUOTE.equals(adapterType)) {
            return (ChronicleQuoteMsg)
                          dc.wire().read(messageType).object();
        }
        return null;
    }


    @Override
    public KdbEnvelope getKdbEnvelope(MessageTypes.AdapterMessageTypes adapterType, int envelopeMaxSize) {
        if (MessageTypes.AdapterMessageTypes.QUOTE.equals(adapterType)) {
            return new KdbQuoteEnvelope(envelopeMaxSize);
        }
        return null;
    }

    @Override
    public KdbMessage mapChronicleToKdbMessage(MessageTypes.AdapterMessageTypes adapterType, ChronicleMessage chronicleMsg) {
        if (MessageTypes.AdapterMessageTypes.QUOTE.equals(adapterType)) {
            return quoteMapper.sourceToDestination((ChronicleQuoteMsg) chronicleMsg);
        }
        return null;
    }

}
