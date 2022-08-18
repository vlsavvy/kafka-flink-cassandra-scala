package com.streaming.services

import com.streaming.model.Car
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.cassandra.CassandraSink

/**
 * CassandraService is a class that sinks DataStream into CassandraDB.
 */
class CassandraService {

  /**
   * Creating environment for Cassandra and sink some Data of car stream into CassandraDB
   *
   * @param sinkCarStream  DataStream of type Car.
   */
  def sinkToCassandraDB(sinkCarStream: DataStream[Car]): Unit = {
    println("Inside sinkTOCassandraDB")

    import org.apache.flink.streaming.api.scala._
    createTypeInformation[(String, Option[Long], Option[Long])]

    //Creating car data to sink into cassandraDB.
    val sinkCarDataStream = sinkCarStream.map(car => (car.Name, car.Cylinders.getOrElse("Null"),
                                                      car.Horsepower.getOrElse("Null")))

    //Open Cassandra connection and Sinking car data into cassandraDB.
    CassandraSink.addSink(sinkCarDataStream)
      .setHost("127.0.0.1")
      .setQuery("INSERT INTO example.car1(Name, Cylinders, Horsepower) values (?, ?,?);")
      .build
  }
}
