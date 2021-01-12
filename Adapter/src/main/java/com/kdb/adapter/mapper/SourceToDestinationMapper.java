package com.kdb.adapter.mapper;

import com.kdb.adapter.messages.ChronicleQuoteMsg;
import com.kdb.adapter.messages.KdbMessage;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Mappings;

@Mapper
public interface SourceToDestinationMapper {

    @Mappings({
            @Mapping(target="time", source="chronicleQuoteMessage.time"),
            @Mapping(target="sym", source="chronicleQuoteMessage.sym"),
            @Mapping(target="bid", source="chronicleQuoteMessage.bid"),
            @Mapping(target="bsize", source="chronicleQuoteMessage.bsize"),
            @Mapping(target="ask", source="chronicleQuoteMessage.ask"),
            @Mapping(target="assize", source="chronicleQuoteMessage.assize"),
            @Mapping(target="bex", source="chronicleQuoteMessage.bex"),
            @Mapping(target="aex", source="chronicleQuoteMessage.aex")
    })
    KdbMessage sourceToDestination(ChronicleQuoteMsg chronicleQuoteMessage);

    ChronicleQuoteMsg destinationToSource(KdbMessage kdbMessage);

}