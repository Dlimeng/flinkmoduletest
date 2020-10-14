package com.lm.flink.datastream.source

import java.util.Properties
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @Classname FlinkKafkaTest
 * @Description TODO
 * @Date 2020/10/14 20:09
 * @Created by limeng
 */
object FlinkKafkaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

   // env.enableCheckpointing(500)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "prd-dcr-01.xbox:9092,prd-data-01.xbox:9092")
    props.setProperty("zookeeper.connect", "prd-hadoop-03.xbox:2181,prd-hadoop-04.xbox:2181,prd-hadoop-05.xbox:2181")
    props.setProperty("group.id", "group-test")

    val consumer = new FlinkKafkaConsumer[String]("fk_string_topic",new SimpleStringSchema(),props)
    consumer.setStartFromEarliest()


    val stream = env.addSource(consumer)

    stream.print()

    env.execute("FlinkKafkaTest")
  }
}
