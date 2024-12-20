package basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import models.Route;
import models.Supplier;
import models.Trip;

import java.util.Properties;
import java.util.Scanner;

public class Producer {
    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        log.info("I am a Producer");

        String bootstrapServers = "kafka_projeto_devcontainer-broker1-1:9092";
        // String TOPIC = "Results"; // Enviar dados para o tópico 'Results'

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "serializer.JSONSerializer");

        KafkaProducer<String, Object> producer = new KafkaProducer<>(properties);

        Scanner scanner = new Scanner(System.in);

       /*
       // Informações do fornecedor (Supplier)
       System.out.println("Supplier ID:");
       String supplierId = scanner.nextLine();
        System.out.println("Supplier Name:");
        String supplierName = scanner.nextLine();
        System.out.println("Supplier Country:");
        String supplierCountry = scanner.nextLine();
        
        Supplier supplier = new Supplier(supplierId, supplierName, supplierCountry);
        sendMessage(producer, "Suppliers", supplier);
        
        // Solicitar informações da rota (Route)
        System.out.println("Route ID: (route-NUM)");
        String routeId = scanner.nextLine();
        System.out.println("Origin:");
        String origin = scanner.nextLine();
        System.out.println("Destination:");
        String destination = scanner.nextLine();
        System.out.println("Passenger Capacity:");
        int passengerCapacity = Integer.parseInt(scanner.nextLine());
        System.out.println("Transport Type:");
        String transportType = scanner.nextLine();
        System.out.println("Operator:");
        String operator = scanner.nextLine();
        
        Route route = new Route(routeId, origin, destination, passengerCapacity, transportType, operator, supplierId,
        passengerCapacity-15);
        sendMessage(producer, "DBInfoTopic-Routes", route);
        
        // Solicitar informações da viagem (Trip)
        System.out.println("Trip ID:");
        String tripId = scanner.nextLine();
        System.out.println("Trip Origin:");
        String tripOrigin = scanner.nextLine();
        System.out.println("Trip Destination:");
        String tripDestination = scanner.nextLine();
        System.out.println("Passenger Name:");
        String passengerName = scanner.nextLine();
        System.out.println("Transport Type for Trip:");
        String tripTransportType = scanner.nextLine();
        
        Trip trip = new Trip(tripId, routeId, tripOrigin, tripDestination, passengerName, tripTransportType);
        sendMessage(producer, "DBInfoTopic-Trips", trip);
        */


        // Exemplo de route
        Route route = new Route("route-1", "Los Angeles", "New York", 200, "Bus", "XYZ Corp", "supplier-1", 50);
        // Exemplo de viagem
        Trip trip = new Trip("trip-1", "route-1", "Los Angeles", "New York", "John Doe", "Bus");
        Supplier supplier = new Supplier("supplier-1", "lino", "portugal");

        sendMessage(producer, "DBInfoTopic-Trips", trip);
        sendMessage(producer, "DBInfoTopic-Routes", route);
        sendMessage(producer, "Suppliers", supplier);


        // Fechar o produtor
        producer.flush();
        producer.close();
        scanner.close();
    }

    // Método para enviar mensagens para o tópico
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
