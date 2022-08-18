package com.knoldus

import com.knoldus.model.Car
import com.knoldus.services.{CassandraService, KafkaService}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
* FlinkKafkaToCassandra is ans  that integrates Flink Streaming
* with kafka and cassandra.
*/
object FlinkKafkaToCassandra {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //Instantiating KafkaService Class and Consume message from kafka topic.
    val carKafkaStream = new KafkaService().kafkaStreamConsumer(environment)
    //Instantiating CassandraService Class and sinking data into CassandraDB.
    val cassandraService = new CassandraService()

   cassandraService.sinkToCassandraDB(carKafkaStream.map(kafkaMessage => {
       println("Receiving Message:"+kafkaMessage)
        val carJsonNode = new ObjectMapper().readValue(kafkaMessage,classOf[JsonNode])
        Car(carJsonNode.get("Name").asText(), Some(carJsonNode.get("Miles_per_Gallon").asDouble()), Some(carJsonNode.get("Cylinders").asLong()),
        Some(carJsonNode.get("Displacement").asDouble()), Some(carJsonNode.get("Horsepower").asLong()), Some(carJsonNode.get("Weight_in_lbs").asLong()),
        Some(carJsonNode.get("Acceleration").asDouble()), carJsonNode.get("Year").asText() , carJsonNode.get("Origin").asText())
    }))
    environment.execute("Flink Kafka to cassandra app")
  }

}
