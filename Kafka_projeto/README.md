# Projeto Kafka

Este projeto tem o objetivo de simular um sistema de mobilidade urbana que depende do kafka para processar dados

Utilizam-se kafka streams para computar diferentes métricas

### Topicos utilizados

Suppliers - Topico que tem dados relativos aos suppliers  
DBInfoTopic-Routes - topico que tem dados relativos às routes todas obtidas da BD  
DBInfoTopic-Trips - topico que tem dados relativos às trips todas obtidas da BD  
Routes - topico que processa routes e gera mais routes (RouterApp) a partir de suppliers  
Trips -  topico que processa routes e gera mais trips  
ResultsTopic-(4 a 16) - topicos com os resultados das kafkaStreams (sao guardados na DB)  

### Funcionamento

RouteApp e TripApp geram novos dados quando lêem dados dos topicos das base de dados
- a RouteApp cria uma nova route quando se recebe um novo supplier no topico "Suppliers" e insere no topico "Routes"
- a TripApp cria uma nova trip quando se recebe uma nova trip no topico "DBInfoTopic-Trips" e insere no topico "Trips"
(Estas configs podem-se mudar facilmente mas para testar é mais simples)

Temos um producer que cria uma route, trip e supplier para os topicos da base de dados para ver estas aplicaçoes a funcionar


KafkaStreams é o programa principal que pega nos dados dos topicos "Routes" e "Trips", junta os dois a partir do id do route ("routeId") que é comum nos dois, e faz o processamento dos dados consoante o que se pretende

Exemplo de formato da tabela:
project3=# select * from "ResultsTopic-teste";
 routeId |   origin    | destination | passengerCapacity | transportType | operator | tripId | passengerName | tripRouteId | tripOrigin  | tripDestination | tripTransportType 
---------+-------------+-------------+-------------------+---------------+----------+--------+---------------+-------------+-------------+-----------------+-------------------
 route-1 | Los Angeles | New York    |               200 | Bus           | XYZ Corp | trip-1 | John Doe      | route-1     | Los Angeles | New York        | Bus


### Configurações

Foi utilizado um docker compose e ficheiros de configuração para configurar os conectores para a base de dados
O formato de comunicação é JSON

