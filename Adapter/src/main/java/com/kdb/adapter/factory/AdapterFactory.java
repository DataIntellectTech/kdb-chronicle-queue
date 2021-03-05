package com.kdb.adapter.factory;

import com.kdb.adapter.mapper.QuoteMapper;
import com.kdb.adapter.messages.*;
import net.openhft.chronicle.wire.DocumentContext;
import org.mapstruct.factory.Mappers;

public class AdapterFactory implements AbstractFactory<ChronicleMessage, KdbEnvelope, KdbMessage> {

  private final QuoteMapper quoteMapper = Mappers.getMapper(QuoteMapper.class);

  @Override
  public ChronicleMessage readChronicleMessage(
      MessageTypes.AdapterMessageTypes adapterType, DocumentContext dc) {
    switch (adapterType) {
      case QUOTE:
        return (ChronicleQuoteMsg) dc.wire().read(adapterType.toString()).object();
      //case TRADE:
      //  return (ChronicleTradeMsg) dc.wire().read(adapterType.toString()).object();
      default:
        return null;
    }
  }

  @Override
  public KdbEnvelope getKdbEnvelope(
      MessageTypes.AdapterMessageTypes adapterType, int envelopeMaxSize) {
    switch (adapterType) {
      case QUOTE:
        return new KdbQuoteEnvelope(envelopeMaxSize);
      //case TRADE:
      //  return new KdbTradeEnvelope(envelopeMaxSize);
      default:
        return null;
    }
  }

  @Override
  public KdbMessage mapChronicleToKdbMessage(
      MessageTypes.AdapterMessageTypes adapterType, ChronicleMessage chronicleMsg) {

    switch (adapterType) {
      case QUOTE:
        return quoteMapper.sourceToDestination((ChronicleQuoteMsg) chronicleMsg);
      //case TRADE:
      //  return (ChronicleTradeMsg) dc.wire().read(adapterType.toString()).object();
      default:
        return null;
    }
  }
}
