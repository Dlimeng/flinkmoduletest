package com.lm.flink.datastream.source

import java.util.Properties

import com.lm.flink.model.WordWithCount
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.connectors.kafka._
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * @Classname FlinkKafkaSource
 * @Description TODO
 * @Date 2020/10/15 18:49
 * @Created by limeng
 */
object FlinkKafkaSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置最少处理一次语义(AT_LEAST_ONCE)和恰好一次语义
    env.enableCheckpointing(5000)

    //每9秒发出一个watermark
    env.getConfig.setAutoWatermarkInterval(9000)

    //设置checkpoint 清楚策略
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "prd-dcr-01.xbox:9092,prd-data-01.xbox:9092")
    props.setProperty("zookeeper.connect", "prd-hadoop-03.xbox:2181,prd-hadoop-04.xbox:2181,prd-hadoop-05.xbox:2181")
    props.setProperty("group.id", "group-test")

    val consumer = new FlinkKafkaConsumer[String]("fk_string_topic",new SimpleStringSchema(),props)
    consumer.setStartFromEarliest()

  //  consumer.setCommitOffsetsOnCheckpoints(true)

    //设置重启策略/5次尝试/每次重试间隔50s
   // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000))

    import org.apache.flink.api.scala._



    env.addSource(consumer).setParallelism(1).flatMap(new FlatMapFunction[String,WordWithCount] {
      override def flatMap(value: String, out: Collector[WordWithCount]): Unit = {
        out.collect(new WordWithCount(value,1L))
      }
    }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[WordWithCount](){
      var currentMaxTimestamp = 0L
      //这个控制失序已经延迟的度量
      val maxOutOfOrderness = 1000L
      //获取当前水位线
      override def getCurrentWatermark: Watermark = {
        val tmpTimestamp = currentMaxTimestamp - maxOutOfOrderness
        //println(s"wall clock is ${System.currentTimeMillis()}  new watermark ${tmpTimestamp}")
        new Watermark(tmpTimestamp)
      }

      override def extractTimestamp(element: WordWithCount, previousElementTimestamp: Long): Long = {
        val timestamp = System.currentTimeMillis()
        currentMaxTimestamp = Math.max(timestamp,currentMaxTimestamp)
        //println(s"get timestamp is $timestamp currentMaxTimestamp $currentMaxTimestamp")
        timestamp
      }
    }).keyBy("word")
      .timeWindow(Time.seconds(20))
      .sum("count").print()


    env.execute("FlinkKafkaSource")
  }
}


