package uk.co.aquaq.kdb.adapter.factory;

import uk.co.aquaq.kdb.adapter.envelopes.KdbEnvelope;
import uk.co.aquaq.kdb.adapter.envelopes.KdbQuoteEnvelope;
import uk.co.aquaq.kdb.adapter.envelopes.KdbTradeEnvelope;
import uk.co.aquaq.kdb.adapter.mapper.QuoteMapper;
import uk.co.aquaq.kdb.adapter.mapper.TradeMapper;
import uk.co.aquaq.kdb.adapter.messages.*;
import net.openhft.chronicle.wire.DocumentContext;
import org.mapstruct.factory.Mappers;

public class AdapterFactory implements AbstractFactory<ChronicleMessage, KdbEnvelope, KdbMessage> {

  private final QuoteMapper quoteMapper = Mappers.getMapper(QuoteMapper.class);
  private final TradeMapper tradeMapper = Mappers.getMapper(TradeMapper.class);

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
