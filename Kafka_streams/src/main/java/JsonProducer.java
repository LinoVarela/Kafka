import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Properties;

public class JsonProducer {

    public static void main(String[] args) {
        String topic = "Example_topic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "broker1:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", JsonSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Criar um JSON a partir de uma String
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode jsonNode = objectMapper.createObjectNode();
        jsonNode.put("message", "Hello, Kafka!");

        // Converter o objeto JSON para string
        String jsonMessage = jsonNode.toString();

        // Enviar a mensagem JSON para o Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, jsonMessage);

        try {
            producer.send(record);
            System.out.println("Mensagem enviada com sucesso.");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
