package com.lm.flink.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random
/**
 * @Classname KafkaProducer
 * @Description TODO
 * @Date 2020/9/23 16:44
 * @Created by limeng
 */
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers","prd-dcr-01.xbox:9092,prd-data-01.xbox:9092")
    props.setProperty("acks", "all")
    props.setProperty("retries", "0")
    props.setProperty("batch.size", "16384")
    props.setProperty("linger.ms", "1")
    props.setProperty("buffer.memory", "33554432")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)

    val producer = new KafkaProducer[String, String](props)
    var random = new Random(2)

    while (true){
      producer.send(new ProducerRecord[String, String]("fk_string_topic", String.valueOf(random.nextInt(100))))
    }

  }



}
