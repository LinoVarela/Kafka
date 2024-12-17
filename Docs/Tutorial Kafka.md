```md
#!/bin/bash

# This document holds some Kafka, Zookeeper and Postgres useful commands

# -----------------------------------------------------------------------------

  

# --- Kafka ---

  

# Runs one producer connected to the bootstrap-server broker1:9092

kafka-console-producer.sh --bootstrap-server broker1:9092 --topic test_topic

  

# Runs one consumer connected to the bootstrap-server broker1:9092

kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic test_topic

  

# Runs one consumer that reads all historical data from the beginning

kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic test_topic --from-beginning

  

# Create one topic with 3 partitions

kafka-topics.sh --bootstrap-server broker1:9092 --create --topic test_topic_with_partitions --partitions 3

kafka-console-producer.sh --bootstrap-server broker1:9092 --topic test_topic_with_partitions

kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic test_topic_with_partitions

  

# Describe a given topic

kafka-topics.sh --bootstrap-server broker1:9092 --describe --topic test_topic_with_partitions

  

# List topics

kafka-topics.sh --bootstrap-server broker1:9092 --list

  

# Test the performance of Kafka

kafka-producer-perf-test.sh --topic test --num-records 10000 --throughput -1 --producer-props bootstrap.servers=broker1:9092 batch.size=1000 acks=1 linger.ms=50 buffer.memory=4294967296 compression.type=none request.timeout.ms=300000 --record-size 1000

  
  

# --- Zookeper ---

  

zookeeper-shell.sh zookeeper:32181

ls /brokers/ids


# --- Postgresql ---

psql -h postgres -p 5432 -U postgres # Login with user postgres

\l               # List databases

\c project3      # Connect to project3 database

\dt              # List tables
```

### Perguntas
##### 1. O que acontece com as mensagens que chegam antes do consumidor subscrever e como configurar para ler todas as mensagens desde o início?
###### Resposta com Comandos Bash:

- Mensagens antes da subscrição: As mensagens que chegam ao tópico antes de um consumidor subscrever ficam disponíveis no tópico até que o retention period expire.
    
- Consumir mensagens históricas desde o início: Use a flag `--from-beginning` no comando do consumidor:
```bash
kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic test_topic --from-beginning
```
Este comando lê todas as mensagens do tópico **test_topic** desde o início, incluindo as históricas.


##### 2. Como configurar diferentes partições em um tópico?

##### Criar Tópico com Partições:
- Use o comando abaixo para criar um tópico com 3 partições:
```bash
kafka-topics.sh --bootstrap-server broker1:9092 --create --topic test_topic_with_partitions --partitions 3
```

- Após criar o tópico, use um producer para enviar mensagens:

```bash
kafka-console-producer.sh --bootstrap-server broker1:9092 --topic test_topic_with_partitions
```

- Executar um consumidor para verificar as mensagens:
```bash
kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic test_topic_with_partitions
```

- Para verificar a configuração do tópico e suas partições:
```bash
kafka-topics.sh --bootstrap-server broker1:9092 --describe --topic test_topic_with_partitions

```


##### 3. Qual é a função dos Consumer Groups? Como eles funcionam?
###### Resposta com Comandos :

- **Consumidores em Grupos**: Um grupo de consumidores divide as partições de um tópico. Mensagens de cada partição são entregues a apenas um consumidor no grupo.
- **Testar Consumer Groups**: Executar múltiplos consumidores com o mesmo grupo ID. Por exemplo:
	```bash
	kafka-console-consumer.sh --bootstrap-server broker1:9092 --topic test_topic_with_partitions --group my_consumer_group

```

...




### More commands or info

```bash
kafka-console-consumer.sh \
  --bootstrap-server kafka_streams_devcontainer-broker1-1:9092 \
  --topic word-count-output \
  --from-beginning \
  --property print.key=true \
  --property print.value=true \
  --property key.separator=":" \
  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
(for streams, with different output topics)


```

Transformar os dados 
```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class ChangeKeyValueTypes {
    public static void main(String[] args) {
        // Configurações do Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "change-key-value-types");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka_streams_devcontainer-broker1-1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Construção do Stream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream("input-topic");

        // Alterar tipos: chave de String para Long e valores de String para Integer
        KStream<Long, Integer> transformedStream = inputStream
                .map((key, value) -> {
                    Long newKey = Long.parseLong(key);    // Converter chave para Long
                    Integer newValue = Integer.parseInt(value); // Converter valor para Integer
                    return new org.apache.kafka.streams.KeyValue<>(newKey, newValue);
                });

        // Enviar o stream convertido para o tópico de saída
        transformedStream.to("output-topic", Produced.with(Serdes.Long(), Serdes.Integer()));

        // Inicializar Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Fechar o Kafka Streams corretamente no encerramento
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

```

```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic input-topic --property parse.key=true --property key.separator=":"

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output-topic --from-beginning --property print.key=true --property print.value=true --property key.separator=":"

```

### Tipos de Operações em Kafka Streams

1. **`reduce`**: Combina valores de mesma chave em uma única operação.
2. **`aggregate`**: Permite acumular valores usando um tipo de dado customizado.
3. **`count`**: Conta o número de ocorrências de cada chave.

Exemplo:
```java
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

public class ReduceExample {
    public static void main(String[] args) {
        // Configurações do Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "reduce-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka_streams_devcontainer-broker1-1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Construção do Stream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream("input-topic");

        // Agrupar os dados por chave e aplicar o reduce para somar os valores
        KGroupedStream<String, String> groupedStream = inputStream.groupByKey();

        groupedStream
                .reduce((aggValue, newValue) -> {
                    int sum = Integer.parseInt(aggValue) + Integer.parseInt(newValue);
                    return String.valueOf(sum);
                }, Materialized.with(Serdes.String(), Serdes.String()))
                .toStream()
                .to("output-topic");

        // Inicializar Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Shutdown hook para fechar o Kafka Streams corretamente
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

```



### Kafka Connect

### **Passos Gerais**

1. **Entender o Kafka Connect**  
    Kafka Connect é uma ferramenta de integração para mover grandes volumes de dados entre Kafka e sistemas externos (como bases de dados) sem escrever código específico.
    
2. **Preparar o ambiente com PostgreSQL**  
    Garantir que a base de dados PostgreSQL está configurada e contém dados que serão exportados para Kafka.
    
3. **Configurar um Kafka Source Connector**  
    Configurar o **JDBC Source Connector**, que lê dados do PostgreSQL e os publica em tópicos Kafka.
    
4. **Verificar os dados no tópico Kafka**  
    Utilizar um consumer para garantir que os dados são corretamente lidos da base de dados e enviados para Kafka.