package uk.co.aquaq.kdb.adapter.factory;

import uk.co.aquaq.kdb.adapter.customexceptions.AdapterConfigurationException;
import uk.co.aquaq.kdb.adapter.mapper.QuoteMapper;
import uk.co.aquaq.kdb.adapter.mapper.TradeMapper;
import uk.co.aquaq.kdb.adapter.messages.*;
import net.openhft.chronicle.wire.DocumentContext;
import org.mapstruct.factory.Mappers;

import java.lang.reflect.Field;

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
  public int filterChronicleMessage(
      ChronicleMessage chronicleMessage,
      MessageTypes.AdapterMessageTypes adapterType,
      String filterField,
      String filterIn,
      String filterOut) {

    try {

      // Keep = 1
      // Reject = 0
      // Error = -1

      // If doesn't match filter return 0
      // If no active filter return 1
      // If matches filter return 1
      // If invalid return -1

      int ret = 0;
      boolean decisionMade = false;

      if (filterIn.length() > 0) {

        if (adapterType.equals(MessageTypes.AdapterMessageTypes.QUOTE)) {
          decisionMade = true;
          ChronicleQuoteMsg msg = (ChronicleQuoteMsg) chronicleMessage;

          ret =
              filterMessageBasedOnStringField(
                  MessageTypes.AdapterMessageTypes.QUOTE.implClass,
                  true,
                  filterField,
                  msg,
                  filterIn);

        } else if (adapterType.equals(MessageTypes.AdapterMessageTypes.TRADE)) {
          decisionMade = true;
          ChronicleTradeMsg msg = (ChronicleTradeMsg) chronicleMessage;

          ret =
              filterMessageBasedOnStringField(
                  MessageTypes.AdapterMessageTypes.TRADE.implClass,
                  true,
                  filterField,
                  msg,
                  filterIn);

        } else {
          throw new AdapterConfigurationException(
              "Adapter type (" + adapterType + ") not configured yet. Check config.");
        }
      } else {
        ret = 1;
      }

      if (filterOut.length() > 0) {

        if (adapterType.equals(MessageTypes.AdapterMessageTypes.QUOTE)) {
          ChronicleQuoteMsg msg = (ChronicleQuoteMsg) chronicleMessage;
          ret =
              filterMessageBasedOnStringField(
                  MessageTypes.AdapterMessageTypes.QUOTE.implClass,
                  false,
                  filterField,
                  msg,
                  filterOut);

        } else if (adapterType.equals(MessageTypes.AdapterMessageTypes.TRADE)) {
          ChronicleTradeMsg msg = (ChronicleTradeMsg) chronicleMessage;
          ret =
              filterMessageBasedOnStringField(
                  MessageTypes.AdapterMessageTypes.TRADE.implClass,
                  false,
                  filterField,
                  msg,
                  filterOut);

        } else {
          throw new AdapterConfigurationException(
              "Adapter type (" + adapterType + ") not configured yet. Check config.");
        }
      } else {
        if (!decisionMade) ret = 1;
      }
      return ret;
    } catch (Exception ex) {
      return -1;
    }
  }

  // Keep = 1
  // Reject = 0
  // Error = -1
  // filterIn = true -> if string check true then keep, else reject
  // filterIn = false -> if string check true then reject, else keep
  private int filterMessageBasedOnStringField(
      String className, boolean filterIn, String filterField, ChronicleMessage msg, String filter) {
    try {
      Class<?> msgClass = Class.forName(className);
      Field field = msgClass.getDeclaredField(filterField);
      field.setAccessible(true);
      String filterFieldValue = (String) field.get(msg);

      int ret = filterIn ? 0 : 1;
      if (filter.contains(filterFieldValue)) {
        ret = filterIn ? 1 : 0;
      }
      return ret;
    } catch (Exception ex) {
      return -1;
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
