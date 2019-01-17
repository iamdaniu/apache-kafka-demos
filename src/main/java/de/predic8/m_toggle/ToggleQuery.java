package de.predic8.m_toggle;

import de.predic8.StreamsUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;

public class ToggleQuery {
    private static final String GLOBAL_TABLE_STORE = "toggle_global_table";
    private static final String STREAM_STORE = "toggle_stream";

    public static void main(String[] args) throws InterruptedException {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "toggle");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        String storeName = createGlobalTable(builder);

        final KafkaStreams streams = StreamsUtil.startStreams(props, builder);
        ReadOnlyKeyValueStore<String, Toggle> store = streams.store(storeName, QueryableStoreTypes.keyValueStore());

        printStoreContents(store);
        streams.close();
    }

    private static String createGlobalTable(StreamsBuilder builder) {
        final GlobalKTable<String, Toggle> globalKTable = builder.globalTable("toggle",
                Materialized.<String, Toggle, KeyValueStore<Bytes, byte[]>>as(GLOBAL_TABLE_STORE).withKeySerde(Serdes.String()).withValueSerde(ToggleSerde.INSTANCE));
        return GLOBAL_TABLE_STORE;
    }

    private static void printStoreContents(ReadOnlyKeyValueStore<String, Toggle> store) {
        System.out.println();
        final KeyValueIterator<String, Toggle> all = store.all();
        while (all.hasNext()) {
            final KeyValue<String, Toggle> next = all.next();
            System.out.printf("%s: %s%n", next.key, next.value);
        }
    }

    private static String createStream(StreamsBuilder builder) {
        final KStream<Long, Toggle> toggle = builder.stream("toggle-stream", Consumed.with(Serdes.Long(), ToggleSerde.INSTANCE));
        return STREAM_STORE;
    }
}



/*

           src.print( Printed.toSysOut());

            src
                .filter((k,v) -> v.getMarkt().equals("Köln"))
                .print( Printed.toSysOut());


                   src
                .filter((k,v) -> v.getMarkt().equals("Köln"))
                .to("koeln", Produced.with(Serdes.Long(),new ToggleSerde()));


                    src
                .filter((k,v) -> v.getMarkt().equals("Köln"))
                .mapValues((k,v) -> v.getMenge())
                .to("koeln", Produced.with(Serdes.Long(),Serdes.Integer()));


                       src
                .filter((k,v) -> v.getMarkt().equals("Köln"))
                .mapValues((k,v) -> v.getMenge())
                .print(Printed.toSysOut());

           src.foreach((k,v) -> {
            System.out.println(v);
        });





        src.map((k,v) -> new KeyValue<>(k,v)).
                peek((k,v) ->
                System.out.println("k  = " + k + " v= " + v)
        ).print(Printed.toSysOut());



           src
                .groupBy((k,v) -> 1)
                .aggregate(
                        () -> 0,
                        (aggKey, neu, agg) -> agg + neu,
                                Materialized.as("store2"))
                                .toStream()
                                .print(Printed.toSysOut());





        src
                .groupBy((k,v) -> 1)
                .windowedBy(SessionWindows.with( ofSeconds(30)))
                .reduce(

                        (agg, neu) -> agg + neu)
                .toStream()
                .print(Printed.toSysOut());



                src
                .through("temp-topic")
                .groupBy((k,v) -> 1)
                .windowedBy(SessionWindows.with( ofSeconds(30)))
                .reduce(

                        (agg, neu) -> agg + neu)
                .toStream()
                .print(Printed.toSysOut());






                KTable byMarkt = src
                .groupBy((k,v) -> v.getMarkt(), Grouped.with(Serdes.String(), new ToggleSerde()))
                .aggregate( () -> 0,
                        (aggKey, neu, agg) -> agg + neu.getMenge(),
                        Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("by-markt").withValueSerde(Serdes.Integer())
                );

        byMarkt.toStream().print(Printed.toSysOut());



        //.to("koeln", Produced.with(Serdes.Long(),Serdes.Integer()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);


        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));



        streams.start();

        Thread.sleep(50000);

        ReadOnlyKeyValueStore<String, Long> store =
                streams.store("by-markt", QueryableStoreTypes.keyValueStore());




        while (true ) {
            System.out.println("Köln: " + store.get("Köln"));
        }


--------


 KTable byMarkt = src
                .groupBy((k,v) -> v.getMarkt(), Grouped.with(Serdes.String(), new ToggleSerde()))
                .aggregate( () -> 0,
                        (aggKey, neu, agg) -> agg + neu.getMenge(),
                        Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("by-markt").withValueSerde(Serdes.Integer())
                );

        byMarkt.toStream().print(Printed.toSysOut());
 */