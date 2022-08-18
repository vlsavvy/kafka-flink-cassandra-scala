package com.streaming.services

import java.util.Properties

import com.streaming.model.Car
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * KafkaService is a class that create kafka consumer.
 */
class KafkaService {

  /**
   * Creating environment for kafka that consume stream message from kafka topic.
   *
   * @param environment  Flink Stream Execution Environment.
   * @return DataStream of type string.
   */
  def kafkaStreamConsumer(environment: StreamExecutionEnvironment): DataStream[String] = {

    import org.apache.flink.streaming.api.scala._

    //Open Kafka connection and Streaming car data through topic.
    val properties:Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "testKafka")
    val kafkaConsumer = new FlinkKafkaConsumer[String]("car.create1", new SimpleStringSchema() , properties)
    environment.addSource(kafkaConsumer)

  }

}
