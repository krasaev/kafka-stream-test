package org;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static final String IN_TOPIC = "in-topic";
    public static final String OUT_TOPIC = "out-topic";
    public static final String PROCESSOR_NAME = "sample-processor";
    public static final String STORE_1 = "store1";
    public static final String STORE_2 = "store2";

    public static void main(String[] args) {
        startProducerStream();
        startConsumerStream();
    }

    public static void startProducerStream() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sample-producer-stream");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "sample-producer-stream-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        streamsConfiguration.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 2);
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
//        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
//        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
//        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.COMPRESSION_TYPE_CONFIG), "gzip");
//        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), "0");
//        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), "0");
//        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");

        Topology topology = new Topology()
                .addSource(IN_TOPIC, Serdes.String().deserializer(), Serdes.ByteArray().deserializer(), IN_TOPIC)
                .addProcessor(PROCESSOR_NAME, () -> new AbstractProcessor<String, byte[]>() {
                    @Override
                    public void process(String key, byte[] value) {
                        KeyValueStore<String, String> store1 = context().getStateStore(STORE_1);
                        KeyValueStore<String, String> store2 = context().getStateStore(STORE_2);
                        String messageId = UUID.randomUUID().toString();
                        store1.put(key, messageId);
                        store2.put(key, messageId);

                        context().headers().add("messageId", messageId.getBytes(StandardCharsets.UTF_8));
                        context().forward(key, value);
                        logger.warn("message was sent {}", messageId);
                    }
                }, IN_TOPIC)
                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_1),
                        Serdes.String(),
                        Serdes.String()), PROCESSOR_NAME)
                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(STORE_2),
                        Serdes.String(),
                        Serdes.String()), PROCESSOR_NAME)
                .addSink(OUT_TOPIC, OUT_TOPIC, Serdes.String().serializer(), Serdes.ByteArray().serializer(), PROCESSOR_NAME);

        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static void startConsumerStream() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sample-consumer-stream");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "sample-consumer-stream-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_committed");
//        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_BETA);

        Topology topology = new Topology()
                .addSource(OUT_TOPIC, Serdes.String().deserializer(), Serdes.ByteArray().deserializer(), OUT_TOPIC)
                .addProcessor(PROCESSOR_NAME, () -> new AbstractProcessor<String, byte[]>() {
                    @Override
                    public void process(String key, byte[] value) {
                        Header messageId = context().headers().lastHeader("messageId");
                        if (messageId == null) {
                            return;
                        }
                        String messageIdString = new String(messageId.value(), StandardCharsets.UTF_8);
                        logger.warn("message was consumed {}", messageIdString);
                    }
                }, OUT_TOPIC);
        KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);

        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
