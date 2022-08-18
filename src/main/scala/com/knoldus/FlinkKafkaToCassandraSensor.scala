package com.knoldus

import com.knoldus.model.{SensorReading}
import com.knoldus.services.{CassandraSensorService, KafkaSensorService, KafkaService}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

/**
* FlinkKafkaToCassandra is ans  that integrates Flink Streaming
* with kafka and cassandra.
*/
object FlinkKafkaToCassandraSensor {

  def main(args: Array[String]): Unit = {

    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //Instantiating KafkaService Class and Consume message from kafka topic.
    val sensorKafkaStream = new KafkaSensorService().kafkaStreamConsumer(environment)
    //Instantiating CassandraSensorService Class and sinking data into CassandraDB.
    val CassandraSensorService = new CassandraSensorService()

   CassandraSensorService.sinkToCassandraDB(sensorKafkaStream.map(kafkaMessage => {
       println("Receiving Sensor Message:"+kafkaMessage)
        val sensorJsonNode = new ObjectMapper().readValue(kafkaMessage,classOf[JsonNode])
        SensorReading(sensorJsonNode.get("id").asText(),sensorJsonNode.get("timestamp").asLong(),sensorJsonNode.get("temperature").asDouble())
    }))
    environment.execute("Flink Kafka to cassandra app")
  }

}
