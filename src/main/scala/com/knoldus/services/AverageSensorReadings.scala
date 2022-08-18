/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.knoldus.services

import com.knoldus.model.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.PropertyAccessor
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerConfig
//import com.fasterxml.jackson.module.scala.DefaultScalaModule


import java.util.Properties

/** Object that defines the DataStream program in the main() method */
object AverageSensorReadings {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    val TOPIC="sensor"

    val props1 = new Properties()
    props1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //props1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer")
    props1.put("schema.registry.url", "http://127.0.0.1:8081")

    val kafkaProducer = new FlinkKafkaProducer[String](
      TOPIC,
      new SimpleStringSchema(),
      props1
    )

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val avgTemp: DataStream[SensorReading] = sensorData
      // convert Fahrenheit to Celsius using an inlined map function
      .map( r =>
      SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)) )
      // organize stream by sensorId
      .keyBy(_.id)
      // group readings in 1 second windows
      .timeWindow(Time.seconds(1))
      // compute average temperature using a user-defined function
      .apply(new TemperatureAverager)

    // print result stream to standard out
    avgTemp.print()

    val objectMapper = new ObjectMapper().registerModule(new SimpleModule())
    objectMapper.setVisibility(PropertyAccessor.ALL, Visibility.NONE)
    objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
    val dsString= avgTemp
      .map{reading=>objectMapper.writeValueAsString(reading)}
    dsString.addSink(kafkaProducer)
    // execute application
    env.execute("Compute average sensor temperature")
  }
}

/** User-defined WindowFunction to compute the average temperature of SensorReadings */
class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(
    sensorId: String,
    window: TimeWindow,
    vals: Iterable[SensorReading],
    out: Collector[SensorReading]): Unit = {

    // compute the average temperature
    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    // emit a SensorReading with the average temperature
    out.collect(SensorReading(sensorId, window.getEnd/1000, avgTemp))
  }
}
