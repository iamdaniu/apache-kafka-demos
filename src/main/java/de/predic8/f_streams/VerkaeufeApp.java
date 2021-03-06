package de.predic8.f_streams;

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

public class VerkaeufeApp {
    private static final String GLOBAL_TABLE_STORE = "verkaeufe_global_table";
    private static final String STREAM_STORE = "verkaeufe_stream";

    public static void main(String[] args) throws InterruptedException {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "verkaeufe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // Wichtig für store Topic
        //        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        String storeName = createGlobalTable(builder);

        final KafkaStreams streams = StreamsUtil.startStreams(props, builder);
        ReadOnlyKeyValueStore<Long, Verkauf> store = streams.store(storeName, QueryableStoreTypes.keyValueStore());

        printStoreContents(store);
        streams.close();
    }

    private static String createGlobalTable(StreamsBuilder builder) {
        final GlobalKTable<Long, Verkauf> globalKTable = builder.globalTable("verkaeufe",
                Materialized.<Long, Verkauf, KeyValueStore<Bytes, byte[]>>as(GLOBAL_TABLE_STORE).withKeySerde(Serdes.Long()).withValueSerde(VerkaufSerde.INSTANCE));
        return GLOBAL_TABLE_STORE;
    }

    private static void printStoreContents(ReadOnlyKeyValueStore<Long, Verkauf> store) {
        System.out.println();
        final KeyValueIterator<Long, Verkauf> all = store.all();
        while (all.hasNext()) {
            final KeyValue<Long, Verkauf> next = all.next();
            System.out.printf("%d: %s%n", next.key, next.value);
        }
    }

    private static String createStream(StreamsBuilder builder) {
        final KStream<Long, Verkauf> verkaeufe = builder.stream("verkaeufe-stream", Consumed.with(Serdes.Long(), VerkaufSerde.INSTANCE));
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
                .to("koeln", Produced.with(Serdes.Long(),new VerkaufSerde()));


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
                .groupBy((k,v) -> v.getMarkt(), Grouped.with(Serdes.String(), new VerkaufSerde()))
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
                .groupBy((k,v) -> v.getMarkt(), Grouped.with(Serdes.String(), new VerkaufSerde()))
                .aggregate( () -> 0,
                        (aggKey, neu, agg) -> agg + neu.getMenge(),
                        Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("by-markt").withValueSerde(Serdes.Integer())
                );

        byMarkt.toStream().print(Printed.toSysOut());
 */