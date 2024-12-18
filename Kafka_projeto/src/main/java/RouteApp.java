
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import models.Route;
import serializer.JSONDeserializer;
import serializer.JSONSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class RouteApp {
    private static final Logger log = LoggerFactory.getLogger(RouteApp.class);

    public static void main(String[] args) {
        final String BOOTSTRAP_SERVER = "localhost:29092,localhost:29093,localhost:29094";
        final String GROUP_ID = "route-consumer";
        final String ROUTE_TOPIC = "Routes";
        final String RESULTS_TOPIC = "Results";

        // Configurações para o consumidor de Routes
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JSONDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configurações para o produtor de Results
        Properties producerProperties = new Properties();
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerializer.class.getName());

        // Criando consumidor e produtor
        KafkaConsumer<String, Route> routeConsumer = new KafkaConsumer<>(consumerProperties);
        KafkaProducer<String, String> resultProducer = new KafkaProducer<>(producerProperties);

        // Subscribe consumidor para o tópico de Routes
        routeConsumer.subscribe(Arrays.asList(ROUTE_TOPIC));

        final Thread mainThread = Thread.currentThread();

        // Shutdown hook para fechar recursos adequadamente
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                routeConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // Processa as Routes
            while (true) {
                ConsumerRecords<String, Route> routeRecords = routeConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Route> record : routeRecords) {
                    log.debug("Received Route: " + record.value());

                    // Calcular ocupação para essa rota
                    String result = calculateOccupancy(record.value());

                    // Enviar resultado para o tópico Results
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(RESULTS_TOPIC, "0", result);
                    resultProducer.send(producerRecord);
                    resultProducer.flush();
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up exception!");

        } catch (Exception e) {
            log.error("Unexpected exception", e);
            e.printStackTrace();

        } finally {
            routeConsumer.close();
            resultProducer.close();
            log.info("Closed RouteApp.");
        }
    }

    // Método para calcular a ocupação (exemplo simples)
    private static String calculateOccupancy(Route route) {
        // Calcular ocupação e retornar como string (exemplo)
        double occupancy = (route.getPassengerCount() / route.getPassengerCapacity()) * 100;
        return String.format("RouteId: %s, Occupancy: %.2f%%", route.getRouteId(), occupancy);
    }
}
