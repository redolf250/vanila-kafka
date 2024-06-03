package com.redolf.node;

import com.redolf.model.StockTransaction;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import static com.redolf.convertor.JsonConverters.stockTickerSerde;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class KTableProcessorNodes {

    public static KTable<String, StockTransaction> parentStockNode(StreamsBuilder builder) {


//        KStream<String, StockTransaction> stockKStream = builder
//                .stream("stock-transactions", Consumed.with(Serdes.String(),stockTickerSerde()));

        KTable<String, StockTransaction> stockKTable = builder
                .table("stock-transactions", Consumed.with(Serdes.String(),stockTickerSerde()));

//        stockKStream.print(Printed.<String, StockTransaction>toSysOut()
//                .withLabel("stock-KStream"));



        stockKTable.toStream().print(Printed.<String, StockTransaction>toSysOut()
                .withLabel("stock-KTable"));


        return stockKTable;
    }
}
