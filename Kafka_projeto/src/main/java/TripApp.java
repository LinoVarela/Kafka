
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import models.Trip;
import serializer.JSONDeserializer;
import serializer.JSONSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TripApp {
    private static final Logger log = LoggerFactory.getLogger(TripApp.class);

    public static void main(String[] args) {
        final String BOOTSTRAP_SERVER = "localhost:29092,localhost:29093,localhost:29094";
        final String GROUP_ID = "trip-consumer";
        final String OUTPUT_TOPIC = "TRIP";
        final String INPUT_TOPIC = "DBInfo";

        // Configurações para o consumidor de Trips
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
        KafkaConsumer<String, Trip> tripConsumer = new KafkaConsumer<>(consumerProperties);
        KafkaProducer<String, String> resultProducer = new KafkaProducer<>(producerProperties);

        // Subscribe consumidor para o tópico de Trips
        tripConsumer.subscribe(Arrays.asList(INPUT_TOPIC));

        final Thread mainThread = Thread.currentThread();

        // Shutdown hook para fechar recursos adequadamente
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                tripConsumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // Processa as Trips
            while (true) {
                ConsumerRecords<String, Trip> tripRecords = tripConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Trip> record : tripRecords) {
                    log.debug("Received Trip: " + record.value());

                    // Calcular número de passageiros para essa viagem (ou outro processamento necessário)
                    String result = calculatePassengerCount(record.value());

                    // Enviar resultado para o tópico Results
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(OUTPUT_TOPIC, "0", result);
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
            tripConsumer.close();
            resultProducer.close();
            log.info("Closed TripApp.");
        }
    }

    // Método para calcular o número de passageiros
    private static String calculatePassengerCount(Trip trip) {
        // Exemplo de cálculo do número de passageiros
        return String.format("TripId: %s,", trip.getTripId());
    }
}
