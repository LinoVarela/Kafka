import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
// import java.util.stream.Stream;
import java.time.Duration;

import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import models.Result;
import models.Route;
import models.Trip;
import serializer.JSONDeserializer;
import serializer.JSONSerializer;

public class KafkaStream {
        private static final Logger log = LoggerFactory.getLogger(KafkaStream.class);

        private static CountDownLatch countDownLatch = new CountDownLatch(1);

        public static void main(String[] args) {

                final String INPUT_TOPIC_ROUTES = "Routes";
                final String INPUT_TOPIC_TRIPS = "Trips";
                final String[] OUTPUT_TOPICS = {
                                "ResultsTopic-4",
                                "ResultsTopic-5",
                                "ResultsTopic-6",
                                "ResultsTopic-7",
                                "ResultsTopic-8",
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

                /*
                 * final String[] OUTPUT_TOPICS_DBINFO = {
                 * "DBInfoTopic-Routes",
                 * "DBInfoTopic-Trips"
                 * };
                 */

                // Configuração do Kafka Streams
                Properties streamsConfiguration = new Properties();
                streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream-processor");
                String bootstrapServers = "kafka_projeto_devcontainer-broker1-1:9092";
                streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG),
                                "earliest");

                // Configuração dos Serializers e Deserializers (com JSON)
                // Trips para enviar para o topico "DBInfoTopic-Trips"
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

                final JSONDeserializer<Result> jsonDeserializerResult = new JSONDeserializer<>();
                final JSONSerializer<Result> jsonSerializerResult = new JSONSerializer<>();

                Map<String, Object> serdePropertiesResult = new HashMap<>();
                Serde<Result> jsonSerdeResult = Serdes.serdeFrom(jsonSerializerResult, jsonDeserializerResult);
                serdePropertiesResult.put("JSONClass", Result.class);
                jsonSerdeResult.configure(serdePropertiesResult, false);

                // -------------------------------------------------------------
                // referência para a thread principal
                final Thread mainThread = Thread.currentThread();

                // Configurar a receção de um SIGINT
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {

                        try {
                                log.info("Received SIGINT");
                                KafkaStream.countDownLatch.countDown();
                                log.info("Unlocked latch");

                                mainThread.join();
                        } catch (InterruptedException e) {
                                e.printStackTrace();
                        }
                }));

                // Configuração do StreamsBuilder
                StreamsBuilder builder = new StreamsBuilder();
                log.info("Setup done for Kafka Serdes and Streams");

                // Consumir dados de "Routes" para criar uma tabela
                KStream<String, Route> routeLines = builder
                                .stream(INPUT_TOPIC_ROUTES, Consumed.with(Serdes.String(), jsonSerdeRoute))
                                .mapValues(v -> {
                                        log.info("Consuming route: " + v);
                                        return v;
                                })
                                .groupBy((key, value) -> value.getRouteId(),
                                                Grouped.with(Serdes.String(), jsonSerdeRoute))
                                .aggregate(
                                                () -> null, // valor inicial
                                                (aggKey, newValue, aggValue) -> newValue, // lógica de agregação
                                                Materialized.with(Serdes.String(), jsonSerdeRoute))
                                .toStream();
                routeLines.peek((k, v) -> log.info("Route Aggregated: " + v));

                // routeLines.to(OUTPUT_TOPICS_DBINFO[0]);

                // Consumir dados de "Trips" para criar uma tabela
                KStream<String, Trip> tripLines = builder
                                .stream(INPUT_TOPIC_TRIPS, Consumed.with(Serdes.String(), jsonSerdeTrip))
                                .mapValues(v -> {
                                        log.info("Consuming trip: " + v);
                                        return v;
                                })
                                .groupBy((key, value) -> value.getRouteId(),
                                                Grouped.with(Serdes.String(), jsonSerdeTrip))
                                .aggregate(
                                                () -> null, // valor inicial
                                                (aggKey, newValue, aggValue) -> newValue, // lógica de agregação
                                                Materialized.with(Serdes.String(), jsonSerdeTrip))
                                .toStream();

                tripLines.peek((k, v) -> log.info("Trip Aggregated: " + v));

                // tripLines.to(OUTPUT_TOPICS_DBINFO[1]);

                // Juntando os dados de Route e Trip para criar resultdao
                KStream<String, Result> joinedStream = routeLines.join(
                                tripLines,
                                (route, trip) -> new Result(route, trip),
                                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                                StreamJoined.with(Serdes.String(), jsonSerdeRoute, jsonSerdeTrip))
                                .peek((key, value) -> log.info("New Result'" + value.getRoute()));

                joinedStream.peek((key, value) -> log.info("Resultado created: " + value));

                // :::::::::::::::::::::::::::::::::::::: REQUISITOS
                // :::::::::::::::::::::::::::::::::

                // 4. Get passengers per route
                KStream<String, Result> passengersPerRoute = joinedStream;
                passengersPerRoute.mapValues((key, result) -> {
                        int passengerCount = result.getRoute().getPassengerCount();
                        return String.format(Locale.US,
                                        "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"field\": \"routeId\", \"type\": \"string\"}, {\"field\": \"passengers\", \"type\": \"int32\"}]}, "
                                                        +
                                                        "\"payload\": {\"routeId\": \"%s\", \"passengers\": %d}}",
                                        result.getRoute().getRouteId(), passengerCount);
                })
                                .to(OUTPUT_TOPICS[0], Produced.with(Serdes.String(), Serdes.String())); // Enviar para
                                                                                                        // "ResultsTopic-4"

                // 5. Get the available seats per route
                KStream<String, Result> availableSeatsPerRoute = joinedStream;
                availableSeatsPerRoute.mapValues((key, result) -> {
                        int availableSeats = result.getRoute().getPassengerCapacity()
                                        - result.getRoute().getPassengerCount();
                        return String.format(Locale.US,
                                        "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"field\": \"routeId\", \"type\": \"string\"}, {\"field\": \"availableSeats\", \"type\": \"int32\"}]}, "
                                                        +
                                                        "\"payload\": {\"routeId\": \"%s\", \"availableSeats\": %d}}",
                                        result.getRoute().getRouteId(), availableSeats);
                })
                                .to(OUTPUT_TOPICS[1], Produced.with(Serdes.String(), Serdes.String())); // Enviar para
                                                                                                        // "ResultsTopic-5"

                // 6. Get the occupancy percentage per route
                KStream<String, Result> occupancyPercentage = joinedStream;
                occupancyPercentage.mapValues((key, result) -> {
                        float percentOccupancy = ((float) result.getRoute().getPassengerCount()
                                        / result.getRoute().getPassengerCapacity()) * 100;
                        return String.format(Locale.US,
                                        "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"field\": \"routeId\", \"type\": \"string\"}, {\"field\": \"occupancyPercentage\", \"type\": \"float\"}]}, "
                                                        +
                                                        "\"payload\": {\"routeId\": \"%s\", \"occupancyPercentage\": %.2f}}",
                                        result.getRoute().getRouteId(), percentOccupancy);
                })
                                .to(OUTPUT_TOPICS[2], Produced.with(Serdes.String(), Serdes.String())); // Enviar para
                                                                                                        // "ResultsTopic-6"

                // 7. Get total passengers per route (TRIPS)
                KStream<String, Long> totalPassengersPerRoute = joinedStream
                                .groupBy((key, result) -> result.getRoute().getRouteId(),
                                                Grouped.with(Serdes.String(), jsonSerdeResult))
                                .aggregate(
                                                () -> 0L,
                                                (routeId, result, total) -> total
                                                                + result.getRoute().getPassengerCount(), // Soma dos
                                                                                                         // passageiros
                                                Materialized.with(Serdes.String(), Serdes.Long()) // Tipos explícitos
                                                                                                  // para chave e valor
                                )
                                .toStream();
                totalPassengersPerRoute
                                .mapValues((key, total) -> String.format(Locale.US,
                                                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"field\": \"routeId\", \"type\": \"string\"}, {\"field\": \"totalPassengers\", \"type\": \"long\"}]}, "
                                                                +
                                                                "\"payload\": {\"routeId\": \"%s\", \"totalPassengers\": %d}}",
                                                key, total))
                                .to(OUTPUT_TOPICS[3], Produced.with(Serdes.String(), Serdes.String())); // Enviar para
                                                                                                        // "ResultsTopic-7"

                // 8. Get total seating available for all routes
                KStream<String, Long> totalAvailableSeats = joinedStream
                                .map((key, result) -> new KeyValue<>(key,
                                                (long) (result.getRoute().getPassengerCapacity()
                                                                - result.getRoute().getPassengerCount()))) // Calcula
                                                                                                           // os
                                                                                                           // assentos
                                                                                                           // disponíveis
                                                                                                           // por
                                                                                                           // rota
                                                                                                           // (e
                                                                                                           // converte
                                                                                                           // para
                                                                                                           // Long)
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long())) // Agrupa pelos keys (routeId)
                                                                                          // e valores
                                                                                          // (assentos disponíveis)
                                .reduce(
                                                (seats1, seats2) -> seats1 + seats2, // Soma os assentos disponíveis de
                                                                                     // todas as rotas
                                                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(
                                                                "total-seats-store") // Especificar
                                                                                     // tipo de
                                                                                     // dados para
                                                                                     // armazenamento
                                                                .withKeySerde(Serdes.String())
                                                                .withValueSerde(Serdes.Long()) // Garantindo que o valor
                                                                                               // será Long
                                )
                                .toStream(); // Converter de volta para KStream para continuar (tentar mudarr)

                totalAvailableSeats
                                .mapValues((totalSeats) -> String.format(
                                                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"field\": \"routeId\", \"type\": \"string\"}, {\"field\": \"totalAvailableSeats\", \"type\": \"long\"}]}, "
                                                                +
                                                                "\"payload\": {\"routeId\": \"%s\", \"totalAvailableSeats\": %d}}",
                                                "route", totalSeats))
                                .to(OUTPUT_TOPICS[4], Produced.with(Serdes.String(), Serdes.String())); // Enviar para
                                                                                                        // "ResultsTopic-8"

                // 9. Get total occupancy percentage (routes)
                KStream<String, Result> occupancyPercentageRoutes = joinedStream;
                occupancyPercentageRoutes.map((key, result) -> new KeyValue<>(key,
                                (float) result.getRoute().getPassengerCount() / result.getRoute().getPassengerCapacity()
                                                * 100)) // Calcula
                                                        // a
                                                        // ocupação
                                                        // de
                                                        // cada
                                                        // rota
                                                        // em %
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Float())) // Agrupa por routeId
                                .reduce(
                                                (occupancy1, occupancy2) -> occupancy1 + occupancy2, // Soma as
                                                                                                     // porcentagens de
                                                                                                     // todas as rotas
                                                Materialized.<String, Float, KeyValueStore<Bytes, byte[]>>as(
                                                                "total-occupancy-store")
                                                                .withKeySerde(Serdes.String())
                                                                .withValueSerde(Serdes.Float()) // Usamos float para a
                                                                                                // ocupação em %
                                )
                                .toStream() // Converte de volta para KStream
                                .mapValues((totalOccupancy) -> String.format(
                                                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"field\": \"routeId\", \"type\": \"string\"}, {\"field\": \"totalOccupancyPercentage\", \"type\": \"float\"}]}, "
                                                                +
                                                                "\"payload\": {\"routeId\": \"%s\", \"totalOccupancyPercentage\": %.2f}}",
                                                totalOccupancy // Formata diretamente o resultado da agregação
                                ))
                                .to(OUTPUT_TOPICS[5], Produced.with(Serdes.String(), Serdes.String())); // Enviar para
                                                                                                        // "ResultsTopic-9"

                // 10. Get the average number of passengers per transport type
                joinedStream
                                .map((key, result) -> new KeyValue<>(result.getRoute().getTransportType(),
                                                result.getRoute().getPassengerCount())) // Cria um par chave-valor: tipo
                                                                                        // de transporte e número
                                                                                        // de passageiros
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer())) // Agrupa por tipo de
                                                                                             // transporte
                                .aggregate(
                                                // Função de inicialização: quando não há valor agregado, iniciamos com
                                                // um
                                                // ArrayList [0, 0] (total, contagem)
                                                () -> {
                                                        ArrayList<Integer> agg = new ArrayList<>();
                                                        agg.add(0); // total de passageiros
                                                        agg.add(0); // contagem
                                                        return agg;
                                                },
                                                // Função de agregação: soma os totais de passageiros e as contagens
                                                (key, value, agg) -> {
                                                        agg.set(0, agg.get(0) + value); // Soma os totais de passageiros
                                                        agg.set(1, agg.get(1) + 1); // Soma a contagem
                                                        return agg;
                                                },
                                                Materialized.with(Serdes.String(),
                                                                Serdes.ListSerde(ArrayList.class, Serdes.Integer())) // Usamos
                                                                                                                     // ListSerde
                                                                                                                     // para
                                                                                                                     // o
                                                                                                                     // ArrayList
                                                                                                                     // de
                                                                                                                     // Integers
                                )
                                .mapValues((key, value) -> value.get(0) / (double) value.get(1)) // Calcula a média de
                                                                                                 // passageiros
                                .toStream() // Converte de volta para KStream para formatar os valores
                                .mapValues((k, v) -> String.format(Locale.US,
                                                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"routeId\"},{\"type\":\"double\",\"optional\":false,\"field\":\"averagePassengers\"}]},\"payload\":{\"routeId\": \"%s\", \"averagePassengers\": %f}}",
                                                k, v)) // Formata como JSON
                                .to(OUTPUT_TOPICS[6], Produced.with(Serdes.String(), Serdes.String())); // Envia para o
                                                                                                        // "ResultsTopic-10"

                // 11. Get the transport type with the highest number of served passengers (only
                // one if there is a tie)
                joinedStream
                                .map((key, result) -> new KeyValue<>(result.getRoute().getTransportType(),
                                                result.getRoute().getPassengerCount())) // Mapeia para o tipo de
                                                                                        // transporte e número de
                                                                                        // passageiros
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer())) // Agrupa por tipo de
                                                                                             // transporte e soma os
                                                                                             // passageiros
                                .aggregate(
                                                // Função de inicialização: inicializa com 0 passageiros
                                                () -> 0L,
                                                // Função de agregação: soma os passageiros
                                                (key, value, aggregate) -> aggregate + value,
                                                // Materialização com os Serdes apropriados
                                                Materialized.with(Serdes.String(), Serdes.Long()))
                                .toStream() // Converte de volta para KStream
                                .map((key, totalPassengers) -> new KeyValue<>(key,
                                                new KeyValue<>(key, totalPassengers))) // Mapeia para
                                                                                       // KeyValue
                                                                                       // com
                                                                                       // transporte
                                                                                       // e total de
                                                                                       // passageiros
                                .mapValues((key, totalPassengers) -> String.format(
                                                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"field\": \"routeId\", \"type\": \"string\"}, {\"field\": \"transportType\", \"type\": \"string\"}, {\"field\": \"totalPassengers\", \"type\": \"long\"}]}, "
                                                                +
                                                                "\"payload\": {\"routeId\": \"%s\", \"transportType\": \"%s\", \"totalPassengers\": %d}}",
                                                "route", key, totalPassengers // Adiciona o tipo de transporte e número
                                                                              // máximo de passageiros
                                ))
                                .to(OUTPUT_TOPICS[7], Produced.with(Serdes.String(), Serdes.String())); // Envia para
                                                                                                        // "ResultsTopic-11"

                // 12. Get the routes with the least occupancy per transport type
                // TO DO

                // 13. Get the most used transport type in the last hour (using a tumbling time
                // window)
                Duration windowDuration = Duration.ofSeconds(30); // Definindo o tamanho da janela para 30 segundos

                joinedStream
                                .map((key, result) -> new KeyValue<>(result.getRoute().getTransportType(),
                                                result.getRoute().getPassengerCount())) // Mapeia para tipo de
                                                                                        // transporte e número de
                                                                                        // passageiros
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer())) // Agrupa por tipo de
                                                                                             // transporte
                                .windowedBy(TimeWindows.of(windowDuration).advanceBy(windowDuration)) // Aplica a janela
                                                                                                      // tumbling de 30
                                                                                                      // segundos
                                .aggregate(
                                                // Função de inicialização: começa com 0 passageiros
                                                () -> 0,
                                                // Função de agregação: soma os passageiros
                                                (key, value, aggregate) -> aggregate + value,
                                                Materialized.with(Serdes.String(), Serdes.Integer()) // Tipos para chave
                                                                                                     // e valor
                                )
                                .toStream()
                                .map((windowedKey, totalPassengers) -> new KeyValue<>(windowedKey.key(),
                                                totalPassengers)) // Mapeia
                                                                  // para
                                                                  // KeyValue
                                                                  // com tipo
                                                                  // de
                                                                  // transporte
                                                                  // e total de
                                                                  // passageiros
                                .mapValues((key, totalPassengers) -> String.format(
                                                "{\"schema\": {\"type\": \"struct\", \"fields\": [{\"field\": \"routeId\", \"type\": \"string\"}, {\"field\": \"transportType\", \"type\": \"string\"}, {\"field\": \"totalPassengers\", \"type\": \"integer\"}]}, "
                                                                +
                                                                "\"payload\": {\"routeId\": \"%s\", \"transportType\": \"%s\", \"totalPassengers\": %d}}",
                                                key, key, totalPassengers // Tipo de transporte, routeId e número de
                                                                          // passageiros
                                ))
                                .to(OUTPUT_TOPICS[9], Produced.with(Serdes.String(), Serdes.String())); // Envia para
                                                                                                        // "ResultsTopic-13"

                /*
                 * 
                 * joinedStream
                 * .mapValues((key, value) -> {
                 * // Serializando para o formato correto para o JDBC Sink Connector
                 * return String.format(
                 * "{\"schema\":{\"type\":\"struct\",\"fields\":["
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"routeId\"},"
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"origin\"},"
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"destination\"},"
                 * + "{\"type\":\"int32\",\"optional\":false,\"field\":\"passengerCapacity\"},"
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"transportType\"},"
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"operator\"},"
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"tripId\"},"
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"passengerName\"},"
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"tripRouteId\"},"
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"tripOrigin\"},"
                 * + "{\"type\":\"string\",\"optional\":false,\"field\":\"tripDestination\"},"
                 * +
                 * "{\"type\":\"string\",\"optional\":false,\"field\":\"tripTransportType\"}]},"
                 * // Adicionando
                 * // campos
                 * // extras
                 * // do
                 * // trip
                 * + "\"payload\":{"
                 * + "\"routeId\":\"%s\","
                 * + "\"origin\":\"%s\","
                 * + "\"destination\":\"%s\","
                 * + "\"passengerCapacity\":%d,"
                 * + "\"transportType\":\"%s\","
                 * + "\"operator\":\"%s\","
                 * + "\"tripId\":\"%s\","
                 * + "\"passengerName\":\"%s\","
                 * + "\"tripRouteId\":\"%s\","
                 * + "\"tripOrigin\":\"%s\","
                 * + "\"tripDestination\":\"%s\","
                 * + "\"tripTransportType\":\"%s\"}}",
                 * value.getRoute().getRouteId(),
                 * value.getRoute().getOrigin(),
                 * value.getRoute().getDestination(),
                 * value.getRoute().getPassengerCapacity(),
                 * value.getRoute().getTransportType(),
                 * value.getRoute().getOperator(),
                 * value.getTrip().getTripId(),
                 * value.getTrip().getPassengerName(),
                 * value.getTrip().getRouteId(),
                 * value.getTrip().getOrigin(),
                 * value.getTrip().getDestination(),
                 * value.getTrip().getTransportType());
                 * })
                 * .to("ResultsTopic-teste", Produced.with(Serdes.String(), Serdes.String()));
                 */

                // Inicializar o KafkaStreams
                KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);

                // Iniciar o Kafka Streams
                streams.start();

                // Adicionar um hook para garantir que o Kafka Streams seja fechado corretamente
                Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        }
}