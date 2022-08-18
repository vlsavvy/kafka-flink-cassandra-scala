package com.knoldus.model.util

//import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import com.fasterxml.jackson.core.JsonProcessingException
import com.knoldus.model.SensorReading
import org.apache.flink.api.common.serialization.SerializationSchema
/*import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule*/
import org.apache.flink.optimizer.plantranslate.JsonMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature
import org.apache.kafka.clients.producer.ProducerRecord

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


@SerialVersionUID(1L)
object SensorRecordSerializationSchema {
  private val objectMapper = new ObjectMapper()
    .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
    false)
}

@SerialVersionUID(1L)
class SensorRecordSerializationSchema extends SerializationSchema[SensorReading] {
  private var topic = "sensor"

  override def serialize(element: SensorReading)= {

    try{
      SensorRecordSerializationSchema.objectMapper.writeValueAsBytes(element)
    }
    catch {
      case e: JsonProcessingException =>
        throw new IllegalArgumentException("Could not serialize record: " + element, e)
    }
  }
}