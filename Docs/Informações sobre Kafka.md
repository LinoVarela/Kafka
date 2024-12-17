
## O que é Apache Kafka?

Apache kafka é uma plataforma distribuída de streaming de dados concebida para lidar com feeds de dados em tempo real. 
É amplamente utilizado para construir sistemas onde aplicações, micro-serviços ou dispositivos precisam de partilhar, ou processar grandes quantidades de dados em tempo real. 


## Core concepts

#### Tópicos 
- É como  um "canal" onde os dados são enviados pelos ***producers*** e lidos pelos ***consumers***. 
- Os tópicos são divididos em partições:
	- As partições permitem que os dados sejam distribuídos para vários consumidores, aumentando a escalabilidade.
	- Cada mensagem numa partição tem um **offset**, que é um identificador único.

#### Partições 

- Cada tópico pode ter várias *partições*, que:
	- Distribuem a carga de trabalho.
	- Garantem que as mensagens em uma partição estão ordenadas (MAS a ordem não é garantida entre diferentes partições).

#### Produtores (*producers*)

- São aplicações ou serviços que enviam mensagens para os tópicos no kafka.
- Podem especificar uma **chave de mensagem** para determinar em que partição a mensagem será armaazenada.

#### Consumidores
- São aplicações ou serviços que subscrevem tópicos para ler as mensagens.
- **<ins>Grupo de consumidores</ins>**:
	- Um grupo de consumidores permite que várias instâncias de consumidores dividam a carga de trabalho de um tópico.
	- O Kafka assegura que cada partição de um tópico é consumida por apenas um consumidor dentro de um grupo.

#### Brokers
- Um broker é um servidor Kafka que armazena dados e lida com pedidos dos clientes (produtores e consumidores)
- Um cluster kafka é composto por vários brokers:
	- Cada partição de um tópico está armazenada num broker, mas é **replicada** noutros para tolerância a falhas.

#### **ZooKeeper/KRaft**

- O Kafka utiliza o **ZooKeeper** (ou o mais recente **KRaft**) para gerir metadados, coordenação do cluster e partilha de estados entre os brokers.

![[kafkaStructure.png]]![[kafkacluster.png]]


## Como funciona o Kafka?

#### Produção de Dados:
- Um producer envia mensagens para um tópico no Kafka.
- As mensagens são atribuídas às partições com base na chave da mensagem ou de forma rotativa.
#### Armazenamento de Dados:
- O Kafka armazena as mensagens nas partições dos tópicos, replicando-as para maior segurança.
#### Consumo de Dados:
- Um consumidor lê as mensagens das partições.
- Os consumidores acompanham o seu progresso utilizando **offsets**.
#### Processamento de Fluxos (Streaming):
- O Kafka streams permite transformar, agregar e analisar os dados em tempo real.


## Kafka Connect

O **Kafka Connect** é uma framework utilizada para integrar o Kafka com outros sistemas externos (BDs, ficheiros, APIs). 
Simplifica o processo mover dados dentro e fora do kafka.

#### Componentes do Kafka Connect:
##### Source Connectors:
- Importa dados de um sistema externo para tópicos Kafka (exemplo: obter linhas de uma tabela de base de dados no Kafka.)
##### Sink Connectors
- Exporta dados do kafka para topicos externos ao sistema (exemplo: escrever dados para uma base de dados)


## Serialização e deserialização em Kafka

Os dados no Kafka são armazenados como fluxos de bytes (stream of bytes). Para interpretar esses bytes, utilizamos processos de serialização (escrever dados em bytes) e deserialização (converter bytes em dados utilizáveis).

##### Formatos Suportados pelo Kafka

1. **Tipos Primitivos**: String, Long, Integer.
2. **JSON**: Legível por humanos e amplamente usado.
3. **Avro**: Compacto e eficiente, ideal para sistemas de larga escala.
4. **Protobuf**: Desempenho elevado e baseado em esquemas.
5. **Serializadores Personalizados**: Implementados para formatos específicos.

## Kafka Streams: Steam processing library

O Kafka Streams é uma biblioteca Java que permite o processamento de dados diretamente nos tópicos Kafka. É usada para transformar, filtrar, juntar e agregar dados.

#### Funcionalidades Principais

1. Processamento sem Estado:
    - Cada mensagem é processada individualmente (ex.: filtrar mensagens).
2. Processamento com Estado:
    - Mantém contexto entre mensagens (ex.: contagem de eventos por chave).
3. Processamento Baseado no Tempo:
    - Janelas temporais como:
        - Janelas Tumbling: Períodos fixos e não sobrepostos.
        - Janelas Deslizantes (Sliding): Períodos que se sobrepõem.

#### Exemplo de Uso

- Contar o número de ocorrências de um evento (ex.: contagem de palavras num fluxo de texto).

---

#### Semânticas de Entrega no Kafka

O Kafka suporta diferentes garantias de entrega de mensagens:

1. No Máximo Uma Vez (At Most Once):
    
    - As mensagens podem ser perdidas, mas nunca entregues em duplicado.
    - Uso: Logs onde a perda de dados é aceitável.
2. Pelo Menos Uma Vez (At Least Once):
    
    - As mensagens nunca são perdidas, mas podem ser entregues em duplicado.
    - Uso: Sistemas que exigem elevada fiabilidade.
3. Exatamente Uma Vez (Exactly Once):
    
    - Cada mensagem é processada apenas uma vez.
    - Requisitos:
        - Produtores Idempotentes: Garantem que as mensagens duplicadas não causam efeitos adversos.
        - Consumidores Transacionais: Gerem os offsets de forma transacional.