package uk.co.aquaq.kdb.adapter.mapper;

import uk.co.aquaq.kdb.adapter.messages.KdbQuoteMessage;
import uk.co.aquaq.kdb.adapter.messages.ChronicleQuoteMsg;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

@Mapper
public interface QuoteMapper {

  @Mappings({
    @Mapping(target = "ts", source = "chronicleQuoteMessage.ts"),
    @Mapping(target = "time", source = "chronicleQuoteMessage.time"),
    @Mapping(target = "sym", source = "chronicleQuoteMessage.sym"),
    @Mapping(target = "bid", source = "chronicleQuoteMessage.bid"),
    @Mapping(target = "bsize", source = "chronicleQuoteMessage.bsize"),
    @Mapping(target = "ask", source = "chronicleQuoteMessage.ask"),
    @Mapping(target = "assize", source = "chronicleQuoteMessage.assize"),
    @Mapping(target = "bex", source = "chronicleQuoteMessage.bex"),
    @Mapping(target = "aex", source = "chronicleQuoteMessage.aex")
  })
  KdbQuoteMessage sourceToDestination(ChronicleQuoteMsg chronicleQuoteMessage);

  ChronicleQuoteMsg destinationToSource(KdbQuoteMessage kdbQuoteMessage);
}
