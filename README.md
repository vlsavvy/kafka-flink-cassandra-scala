# kafka-flink-cassandra-scala

A basic template of integrating Flink streaming with Kafka and CassandraDB using scala as a language.

Table of contents
-----------------
- Getting Started
- Running
- Result

Getting Started - Minimum requirements
=====================================
To run this example you will need Java 1.8+, scala 2.11.2, Flink 1.9.0 , Kafka 2.3.0 , Cassandra 3.10.

Running:
=======

Start Cassandra:
===============
% export CASSANDRA_HOME=/Users/<userxyz>/Documents/Other/apache-cassandra-3.10-src 
bin % ./cassandra -f  

Note:
if required in cassandra-env conf file - change the CASSANDRA_HOME/lib o libexec - not required if executing from the cassandra path n clean installation of cassandra

Start cqlsh:
===========
- bin % cd /Users/<userxyz>/Documents/Other/apache-cassandra-3.10/bin
- bin % cqlsh

- Create keyspace and tables:
- CREATE KEYSPACE example WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor':1};
- CREATE TABLE example.car1(Name text primary key, Cylinders bigint, Horsepower bigint);
- CREATE TABLE example.sensor_reading(id text primary key, Timestamp bigint, Temperature double);

Kafka:
=====
Unzip kafka tarball and navigate to the installed path

> kafka_2.12-3.2.1 % bin/zookeeper-server-start.sh config/zookeeper.properties
> bin/kafka-server-start.sh config/server.properties
> ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic car.create1 //manually paste the json messages in the prompt
>>>>> {"Name":"99e", "Miles_per_Gallon":25, "Cylinders":4, "Displacement":104, "Horsepower":95, "Weight_in_lbs":2375, "Acceleration":17.5, "Year":"1970-01-01", "Origin":"USA"}

> ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor //AverageSensor programmatically pushes to this topic

Additional helper commands:
==========================
> bin/kafka-consumer-groups.sh --list --bootstrap-server localhost:9092
> bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic car.create1 --from-beginning
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic car.create1

Kafdrop - Kafka UI:
==================
Other % /Users/<userxyz>/Documents/Other/jdk-18.0.2.jdk/Contents/Home/bin/java -jar ./kafdrop.jar --kafka.javabrokerConnect=localhost:9092


Running Flink application: (FlinkKafkaToCassandra , FlinkKafkaToCassandraSensor)
==========================
Go inside the project and open a terminal and run the below commands:

sbt clean compile
sbt run

Produce some sample messages in the kafka topic kafkaToCassandra
================================================================

{"Name":"saab 99e", "Miles_per_Gallon":25, "Cylinders":4, "Displacement":104, "Horsepower":95, "Weight_in_lbs":2375, "Acceleration":17.5, "Year":"1970-01-01", "Origin":"Europe"} {"Name":"amc gremlin", "Miles_per_Gallon":21, "Cylinders":6, "Displacement":199, "Horsepower":90, "Weight_in_lbs":2648, "Acceleration":15, "Year":"1970-01-01", "Origin":"USA"} {"Name":"chevy c20", "Miles_per_Gallon":10, "Cylinders":8, "Displacement":307, "Horsepower":200, "Weight_in_lbs":4376, "Acceleration":15, "Year":"1970-01-01", "Origin":"USA"}

Result
======
Go to the cassandra shell and run the below command:

select * from example.car;
You will get Name of the cars, Number of Cylinders used, and Horsepower of a cars into the cassandra Database that streams from kafka.

select * from example.sensor_reading:
You will see the sensor readings persisted  - This example shows us how flink can produce messages to Kafka topic "sensor" and also how to consume from kafka.
