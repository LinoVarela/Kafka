import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.Properties;

public class EventCountExample {

    public static void main(String[] args) {
        // Configurações do Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "event-count-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka_streams_devcontainer-broker1-1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Construtor de Streams
        StreamsBuilder builder = new StreamsBuilder();

        // Stream de entrada - Lê mensagens do tópico de entrada
        KStream<String, String> inputStream = builder.stream(
                "event-input", 
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // Contagem de ocorrências por chave
        KTable<String, Long> eventCounts = inputStream
                .groupBy((key, value) -> key) // Agrupa por chave
                .count();                    // Conta os eventos para cada chave

        // Converte a contagem de Long para String e escreve no tópico de saída
        eventCounts
                .toStream() // Converte KTable de volta para KStream
                .mapValues(value -> "Count: " + value) // Converte Long -> String
                .to("event-output", Produced.with(Serdes.String(), Serdes.String()));

        // Inicializa o Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Adiciona um hook para fechar o Kafka Streams corretamente
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
