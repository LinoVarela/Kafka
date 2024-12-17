import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class WordCountExample {
    private static final Logger log = LoggerFactory.getLogger(WordCountExample.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        String bootstrapServers = "kafka_streams_devcontainer-broker1-1:9092";

        // Configurações principais
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Construção do fluxo
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("word-count-input", Consumed.with(Serdes.String(), Serdes.String()));

        textLines.peek((key, value) -> log.info("Processing input: key={}, value={}", key, value))
                 .flatMapValues(value -> List.of(value.toLowerCase().split("\\W+")))
                 .peek((key, value) -> log.info("Processed word: {}", value))
                 .groupBy((key, value) -> value)
                 .count()
                 .toStream()
                 .peek((key, value) -> log.info("Output result: key={}, value={}", key, value))
                 .to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        // Inicializar Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Shutdown hook para fechar o Kafka Streams corretamente
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
