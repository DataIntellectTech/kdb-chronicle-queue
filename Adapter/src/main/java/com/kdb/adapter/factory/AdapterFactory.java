package com.kdb.adapter.factory;

import com.kdb.adapter.AdapterApplication;
import com.kdb.adapter.mapper.QuoteMapper;
import com.kdb.adapter.mapper.TradeMapper;
import com.kdb.adapter.messages.*;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.DocumentContext;
import org.mapstruct.factory.Mappers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdapterFactory implements AbstractFactory<ChronicleMessage, KdbEnvelope, KdbMessage> {

  private final QuoteMapper quoteMapper = Mappers.getMapper(QuoteMapper.class);
  private final TradeMapper tradeMapper = Mappers.getMapper(TradeMapper.class);
  private static Logger log = LoggerFactory.getLogger(AdapterFactory.class);

  @Override
  public ChronicleMessage readChronicleMessage(
      MessageTypes.AdapterMessageTypes adapterType, DocumentContext dc) {
    switch (adapterType) {
      case QUOTE:
        return (ChronicleQuoteMsg) dc.wire().read(adapterType.toString()).object();
      case TRADE:
        return (ChronicleTradeMsg) dc.wire().read(adapterType.toString()).object();
      default:
        return null;
    }
  }

  @Override
  public int filterChronicleMessage(
      ChronicleMessage chronicleMessage,
      MessageTypes.AdapterMessageTypes adapterType,
      String filter) {

    // If doesn't match filter return 0
    // If no active filter return 1
    // If matches filter return 1
    // If invalid return -1

    int ret = 0;

    if (adapterType.equals(MessageTypes.AdapterMessageTypes.QUOTE)) {

      ChronicleQuoteMsg msg = (ChronicleQuoteMsg) chronicleMessage;
      if (filter.length() == 0) {
        ret = 1;
      } else if ((filter.length() > 0) && (filter.indexOf(msg.getSym()) != -1)) {
        ret = 1;
      } else ret = 0;
    } else if (adapterType.equals(MessageTypes.AdapterMessageTypes.TRADE)) {

      ChronicleTradeMsg msg = (ChronicleTradeMsg) chronicleMessage;
      if (filter.length() == 0) {
        ret = 1;
      } else if ((filter.length() > 0) && (filter.indexOf(msg.getSym()) != -1)) {
        ret = 1;
      } else ret = 0;
    } else {
      ret = -1;
    }
    return ret;
  }

  @Override
  public KdbEnvelope getKdbEnvelope(
      MessageTypes.AdapterMessageTypes adapterType, int envelopeMaxSize) {
    switch (adapterType) {
      case QUOTE:
        return new KdbQuoteEnvelope(envelopeMaxSize);
      case TRADE:
        return new KdbTradeEnvelope(envelopeMaxSize);
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
      case TRADE:
        return tradeMapper.sourceToDestination((ChronicleTradeMsg) chronicleMsg);
      default:
        return null;
    }
  }
}
