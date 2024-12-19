import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import models.Supplier;
import models.Trip;
import serializer.JSONDeserializer;
import serializer.JSONSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class TripApp {
    private static final Logger log = LoggerFactory.getLogger(TripApp.class);

    private static final AtomicInteger tripCounter = new AtomicInteger(1);

    public static void main(String[] args) {
        String bootstrapServers = "kafka_projeto_devcontainer-broker1-1:9092";
        final String GROUP_ID = "my-fourth-application";
        final String OUTPUT_TOPIC = "Trips";
        final String INPUT_TOPIC = "DBInfoTopic-Trip";

        // Configurações para o consumidor de DBInfo
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,JSONDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put("JSONClass", Trip.class);

        
        // Configurações para o produtor de Trips
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerializer.class.getName());

        // Criando consumidor e produtor
        KafkaConsumer<String, Trip> tripConsumer = new KafkaConsumer<>(consumerProperties);
        KafkaProducer<String, Trip> tripProducer = new KafkaProducer<>(producerProperties);

        // Subscribe consumidor para o tópico de DBInfo
        tripConsumer.subscribe(Arrays.asList(INPUT_TOPIC));

        final Thread mainThread = Thread.currentThread();

        // Shutdown hook para fechar recursos adequadamente
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            tripConsumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            while (true) {
                // Consome mensagens do tópico DBInfo
                ConsumerRecords<String, Trip> tripRecords = tripConsumer.poll(Duration.ofMillis(100));
                System.out.println("Here2");
                for (ConsumerRecord<String, Trip> record : tripRecords) {
                    System.out.println("Here3");
                    log.debug("Received Trip: {}", record.value());

                    // Processar a mensagem e gerar ID único no formato `trip-1`, `trip-2`...
                    Trip processedTrip = generateTripWithUniqueId(record.value());

                    // Produz para o tópico Trips
                    ProducerRecord<String, Trip> producerRecord = new ProducerRecord<>(OUTPUT_TOPIC,
                            processedTrip.getTripId(), processedTrip);
                    tripProducer.send(producerRecord);
                    log.debug("Produced Trip: {}", processedTrip);

                    // Flush (opcional para garantia de envio imediato)
                    tripProducer.flush();
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up exception triggered. Exiting gracefully...");
        } catch (Exception e) {
            log.error("Unexpected exception occurred", e);
        } finally {
            tripConsumer.close();
            tripProducer.close();
            log.info("Closed TripApp.");
        }
    }

    // Método para gerar Trips com IDs únicos no formato desejado
    private static Trip generateTripWithUniqueId(Trip trip) {
        return new Trip(
                "trip-" + tripCounter.getAndIncrement(),
                trip.getRouteId(),
                trip.getOrigin(),
                trip.getDestination(),
                trip.getPassengerName(),
                trip.getTransportType());
    }
}
