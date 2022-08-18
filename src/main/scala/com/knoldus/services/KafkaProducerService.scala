package com.knoldus.services

import com.knoldus.model.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.PropertyAccessor
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

class KafkaProducerService {
  import java.util.Properties

  def publishToTopic(sensorReadingDs:DataStream[SensorReading]): Unit = {
    val TOPIC="sensor"

    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val kafkaProducer = new FlinkKafkaProducer[String](
      "localhost:9092",
      TOPIC,
      new SimpleStringSchema()
    )

    val objectMapper = new ObjectMapper().registerModule(new SimpleModule())
    objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE)
    objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
    val dsString= sensorReadingDs
      .map{reading=>objectMapper.writeValueAsString(reading)}
    dsString.addSink(kafkaProducer)  }
}
