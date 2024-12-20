import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.time.Duration;

import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import models.Result;
import models.Route;
import models.Trip;
import serializer.JSONDeserializer;
import serializer.JSONSerializer;

public class KafkaStreamProcessor {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamProcessor.class);

    public static void main(String[] args) {

        final String INPUT_TOPIC_ROUTES = "Routes";
        final String INPUT_TOPIC_TRIPS = "Trips";
        final String[] OUTPUT_TOPICS = {
                "ResultsTopic-4", // Passengers per route
                "ResultsTopic-5", // Available seats per route
                "ResultsTopic-6", // Occupancy percentage per route
                "ResultsTopic-7", // Total passengers
                "ResultsTopic-8", // Total available seating
                "ResultsTopic-9",
                "ResultsTopic-10",
                "ResultsTopic-11",
                "ResultsTopic-12",
                "ResultsTopic-13",
                "ResultsTopic-14",
                "ResultsTopic-15",
                "ResultsTopic-16",
                "ResultsTopic-17"
        };

        // Configuração do Kafka Streams
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-processor");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka_projeto_devcontainer-broker1-1:9092");
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
                
        // Configuração do StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();
        log.info("Starting Kafka Streams application");
        log.debug("Setup done for Kafka Serdes and Streams");

        // Configuração dos Serializers e Deserializers (com JSON)
        final JSONDeserializer<Route> jsonDeserializerRoute = new JSONDeserializer<>();
        final JSONSerializer<Route> jsonSerializerRoute = new JSONSerializer<>();

        Map<String, Object> serdePropertiesRoute = new HashMap<>();
        Serde<Route> jsonSerdeRoute = Serdes.serdeFrom(jsonSerializerRoute, jsonDeserializerRoute);
        serdePropertiesRoute.put("JSONClass", Route.class);
        jsonSerdeRoute.configure(serdePropertiesRoute, false);

        final JSONDeserializer<Trip> jsonDeserializerTrip = new JSONDeserializer<>();
        final JSONSerializer<Trip> jsonSerializerTrip = new JSONSerializer<>();

        Map<String, Object> serdePropertiesTrip = new HashMap<>();
        Serde<Trip> jsonSerdeTrip = Serdes.serdeFrom(jsonSerializerTrip, jsonDeserializerTrip);
        serdePropertiesTrip.put("JSONClass", Trip.class);
        jsonSerdeTrip.configure(serdePropertiesTrip, false);


        log.debug("Setup done for Kafka Serdes and Streams");

        // Consumindo dados de "Routes" para criar uma tabela
        KStream<String, Route> routeLines = builder.stream(INPUT_TOPIC_ROUTES, Consumed.with(Serdes.String(), jsonSerdeRoute))
                .mapValues(v -> {
                    log.debug("Consuming route: " + v);
                    return v; 
                })
                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.String(), jsonSerdeRoute))
                .aggregate(
                        () -> null, // valor inicial
                        (aggKey, newValue, aggValue) -> newValue, // lógica de agregação
                        Materialized.with(Serdes.String(), jsonSerdeRoute))
                .toStream();

        routeLines.peek((k, v) -> log.debug("Route Aggregated: " + v));

        // Consumindo dados de "Trips" para criar uma tabela
        KStream<String, Trip> tripLines = builder.stream(INPUT_TOPIC_TRIPS, Consumed.with(Serdes.String(), jsonSerdeTrip))
                .mapValues(v -> {
                    log.debug("Consuming trip: " + v);
                    return v; 
                })
                .groupBy((key, value) -> value.getRouteId(), Grouped.with(Serdes.String(), jsonSerdeTrip))
                .aggregate(
                        () -> null, // valor inicial
                        (aggKey, newValue, aggValue) -> newValue, // lógica de agregação
                        Materialized.with(Serdes.String(), jsonSerdeTrip))
                .toStream();

        tripLines.peek((k, v) -> log.debug("Trip Aggregated: " + v));
        
        // Juntando os dados de Route e Trip para criar resultdao
        KStream<String, Result> joinedStream = routeLines.join(
                tripLines,
                (route, trip) -> new Result(route, trip),
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                StreamJoined.with(Serdes.String(), jsonSerdeRoute, jsonSerdeTrip))
                .peek((key, value) -> log.debug("New Result'" + value.getRoute()));
                
                joinedStream.peek((key, value) -> log.debug("Resultado created: " + value));
                
                /*




        
        // REQUISITOS 

        //5. Get the available seats per route
        joinedStream
        .mapValues((key, result) -> {
                int availableSeats = result.getRoute().getPassengerCapacity() - result.getRoute().getPassengerCount();
                return String.format("{\"routeId\":\"%s\",\"availableSeatsss\":%d}",
                result.getRoute().getRouteId(), availableSeats);
        })
        .to(OUTPUT_TOPICS[1], Produced.with(Serdes.String(), Serdes.String())); // Enviar para "ResultsTopic-5"
        
        
        */

        /* 
         * 
         // Processando e enviando para múltiplos tópicos de resultados
         joinedStream
         .mapValues((key, result) -> {
                    // Exemplo de processamento e formatação para ocupação
                    String resultData = String.format(
                            "{\"routeId\":\"%s\",\"occupancyPercentage\":%.2f}",
                            result.getRoute().getRouteId(),
                            (double) result.getRoute().getPassengerCount() / result.getRoute().getPassengerCapacity()
                            * 100);
                    return resultData;
                })
                .to(OUTPUT_TOPICS[2], Produced.with(Serdes.String(), Serdes.String())); // Enviar para o
                // "ResultsTopic-6"
                
                // Enviar total de passageiros para um tópico separado
                joinedStream
                .mapValues((key, result) -> {
                        // Exemplo de total de passageiros por rota
                        return String.format("{\"routeId\":\"%s\",\"totalPassengers\":%d}",
                        result.getRoute().getRouteId(), result.getTrip().getPassengerName());
                })
                .to(OUTPUT_TOPICS[3], Produced.with(Serdes.String(), Serdes.String())); // Enviar para o
                // "ResultsTopic-7"
                
                // Enviar a capacidade total de assentos
                joinedStream
                .mapValues((key, result) -> {
                        // Exemplo de capacidade total de assentos
                        return String.format("{\"routeId\":\"%s\",\"totalSeats\":%d}",
                        result.getRoute().getRouteId(), result.getRoute().getPassengerCapacity());
                })
                .to(OUTPUT_TOPICS[4], Produced.with(Serdes.String(), Serdes.String())); // Enviar para o
                // "ResultsTopic-8"
                
                /*
                */
                // Enviar dados para o tópico "Results" no formato esperado pelo JDBC Sink
        // Connector
        joinedStream
        .mapValues((key, value) -> {
        // Serializando para o formato correto para o JDBC Sink Connector
        return String.format(
        "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"routeId\"},"
        +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"origin\"}," +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"destination\"}," +
        "{\"type\":\"int32\",\"optional\":false,\"field\":\"passengerCapacity\"}," +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"transportType\"}," +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"operator\"}," +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"tripId\"}," +
        "{\"type\":\"string\",\"optional\":false,\"field\":\"passengerName\"}]}," +
        "\"payload\":{\"routeId\":\"%s\",\"origin\":\"%s\",\"destination\":\"%s\",\"passengerCapacity\":%d,"
        +
        "\"transportType\":\"%s\",\"operator\":\"%s\",\"tripId\":\"%s\",\"passengerName\":\"%s\"}}",
        value.getRoute().getRouteId(),
        value.getRoute().getOrigin(),
        value.getRoute().getDestination(),
        value.getRoute().getPassengerCapacity(),
        value.getRoute().getTransportType(),
        value.getRoute().getOperator(),
        value.getTrip().getTripId(),
        value.getTrip().getPassengerName());
        })
        .to("Results", Produced.with(Serdes.String(), Serdes.String()));
        
        
        // Inicializar o KafkaStreams
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        
        // Iniciar o Kafka Streams
        streams.start();
        
        // Adicionar um hook para garantir que o Kafka Streams seja fechado corretamente
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
}
}
