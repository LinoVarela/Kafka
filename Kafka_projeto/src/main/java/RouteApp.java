import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import models.Route;
import models.Supplier;
import serializer.JSONDeserializer;
import serializer.JSONSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class RouteApp {
    private static final Logger log = LoggerFactory.getLogger(RouteApp.class);

    private static final AtomicInteger routeCounter = new AtomicInteger(1);

    // Geração de valores aleatórios para simular rotas
    public static Route generateRandomRoute(String supplierId) {
        String[] origins = { "CityA", "CityB", "CityC", "CityD" };
        String[] destinations = { "CityX", "CityY", "CityZ", "CityW" };
        String[] transportTypes = { "Bus", "Train", "Metro", "Taxi", "Scooter" };
        String operator = "Operator" + (int) (Math.random() * 10);

        String origin = origins[(int) (Math.random() * origins.length)];
        String destination;
        do {
            destination = destinations[(int) (Math.random() * destinations.length)];
        } while (destination.equals(origin)); // Evitar que origem e destino sejam iguais
        int passengerCapacity = (int) (Math.random() * 200) + 1;

        return new Route(
                "route-" + routeCounter.getAndIncrement(),
                origin,
                destination,
                passengerCapacity,
                transportTypes[(int) (Math.random() * transportTypes.length)],
                operator,
                supplierId);
    }

    public static void main(String[] args) {
        String bootstrapServers = "kafka_projeto_devcontainer-broker1-1:9092";
        final String groupId = "my-route-application";
        final String INPUT_TOPIC = "DBInfo";
        final String OUTPUT_TOPIC = "Routes";

        // Configurações do consumidor
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                JSONDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configurações do produtor
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerializer.class.getName());

        KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(consumerProperties);
        KafkaProducer<String, Route> producer = new KafkaProducer<>(producerProperties);

        // Subscrição do consumidor no tópico DBInfo
        consumer.subscribe(Arrays.asList(INPUT_TOPIC));

        System.out.println("HERE1");
        final Thread mainThread = Thread.currentThread();

        // Adiciona shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected shutdown. Closing consumer...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            while (true) {
                ConsumerRecords<String, Supplier> records = consumer.poll(Duration.ofMillis(100));
                System.out.println("HERE2");
                for (ConsumerRecord<String, Supplier> record : records) {
                    System.out.println("HERE 3");
                    log.debug("Received Supplier: {}", record.value());

                    // Gerar uma rota aleatória baseada no Supplier recebido
                    Route randomRoute = generateRandomRoute(record.value().getSupplierId());

                    // Produzir a rota gerada no tópico Routes
                    ProducerRecord<String, Route> producerRecord = new ProducerRecord<>(OUTPUT_TOPIC,
                            randomRoute.getRouteId(), randomRoute);
                    producer.send(producerRecord);
                    log.debug("Produced Route: {}", randomRoute);

                    // Flush (opcional)
                    producer.flush();
                }
            }
        } catch (WakeupException e) {
            log.info("Wakeup exception triggered. Exiting...");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            consumer.close();
            producer.close();
            log.info("Closed RouteApp.");
        }
    }
}
