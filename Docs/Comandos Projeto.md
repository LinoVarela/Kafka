
#### Comandos para o connect

```bash

curl -X GET http://localhost:8083/connectors


```

NO CAMINHO CORRETO:
```bash
curl -X POST -H "Content-Type: application/json" --data @sink.json http://localhost:8083/connectors

curl -X POST -H "Content-Type: application/json" --data @source.json http://localhost:8083/connectors


curl -X GET http://localhost:8083/connectors/jdbc-source-suppliers-example/status
curl -X GET http://localhost:8083/connectors/jdbc-postgresql-sink/status
```
Base de dados:
```sql
psql -h localhost -p 5432 -U postgres -d project3
```


##### Criar tópicos 
```bash
kafka-topics.sh --bootstrap-server broker1:9092 --create --topic DBInfo  --partitions 3

kafka-topics.sh --bootstrap-server broker1:9092 --create --topic Routes  --partitions 3

kafka-topics.sh --bootstrap-server broker1:9092 --create --topic Trips  --partitions 3

kafka-topics.sh --bootstrap-server broker1:9092 --create --topic Resutls  --partitions 3

```
#####