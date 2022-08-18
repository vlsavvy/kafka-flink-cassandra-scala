package com.knoldus.services

import com.knoldus.model.{SensorReading}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

/**
 * CassandraService is a class that sinks DataStream into CassandraDB.
 */
class CassandraSensorService {

  /**
   * Creating environment for Cassandra and sink some Data of sensor stream into CassandraDB
   *
   * @param sinkSensorStream  DataStream of type Sensor.
   */
  def sinkToCassandraDB(sinkSensorStream: DataStream[SensorReading]): Unit = {
    println("Inside sinkTOCassandraDB")

    import org.apache.flink.streaming.api.scala._
    createTypeInformation[(String, Option[Long], Option[Long])]

    //Creating sensor data to sink into cassandraDB.
    val sinkSensorDataStream = sinkSensorStream.map(sensor => (sensor.id, sensor.timestamp,
                                                      sensor.temperature))

    //Open Cassandra connection and Sinking sensor data into cassandraDB.
    CassandraSink.addSink(sinkSensorDataStream)
      .setHost("127.0.0.1")
      .setQuery("INSERT INTO example.sensor_reading(Id, timestamp, temperature) values (?, ?,?);")
      .build
  }
}
