package com.lm.flink.datastream.windows

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


/**
 * @Classname TumblingWindowsProcessFunction
 * @Description TODO
 * @Date 2020/9/23 18:14
 * @Created by limeng
 */
object TumblingWindowsProcessFunction_tmp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000) //watermark间隔时间

    //设置事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers","prd-dcr-01.xbox:9092,prd-data-01.xbox:9092")
    props.setProperty("group.id", "group-test")


    /**
     * FlinkKafkaConsumer  配置partition 被消费的offset起始位
     * setStartFromLatest 即从最早的/最新的消息开始消费
     * setStartFromGroupOffsets(默认) 采用consumer group的offset来作为起始位，这个offset从Kafka brokers(0.9以上版本) 或 Zookeeper(Kafka 0.8)中获取。如果从Kafka brokers或者Zookeeper中找不到这个consumer group对应的partition的offset，那么auto.offset.reset这个配置就会被启用。
     *
     */
    val kafkaConsumer =  new FlinkKafkaConsumer("fk_string_topic",new SimpleStringSchema(),props)
      .setStartFromLatest()



    import org.apache.flink.api.scala._


  }

 // class MyProcessWindowFunction extends
}
