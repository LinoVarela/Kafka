import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import models.Route;
import models.Trip;
import java.util.Properties;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        log.info("I am a Producer");

        String bootstrapServers = "kafka_projeto_devcontainer-broker1-1:9092";
        String TOPIC = "Results"; // Enviar dados para o tópico 'Results'

        // Criação das propriedades do produtor
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "serializer.JSONSerializer");

        // Criando o produtor Kafka
        KafkaProducer<String, Object> producer = new KafkaProducer<>(properties);

        // Criando dados de exemplo que irão para o tópico

        // Exemplo de rota
        Route route = new Route("route-1", "Los Angeles", "New York", 200, "Bus", "XYZ Corp");

        // Exemplo de viagem
        Trip trip = new Trip("trip-1", "route-1", "Los Angeles", "New York", "John Doe", "Bus");

        // Enviar os dados para o tópico 'Results'
        sendMessage(producer, TOPIC, route);
        sendMessage(producer, TOPIC, trip);

        // Fechar o produtor
        producer.flush();
        producer.close();
    }

    // Método para enviar mensagens para o tópico 'Results'
    private static void sendMessage(KafkaProducer<String, Object> producer, String topic, Object message) {
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic, "key1", message);
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                log.info("Sent message to topic '{}': Key = {}, Value = {}, Partition = {}, Offset = {}",
                        metadata.topic(), "key1", message, metadata.partition(), metadata.offset());
            } else {
                log.error("Error while producing message", exception);
            }
        });
    }
}
