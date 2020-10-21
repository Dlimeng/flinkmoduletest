package com.lm.flink.datastream.process

import java.util.Properties

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, ProcessFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

/**
 * @Classname ProcessFunctionTimeOut
 * @Description TODO
 * @Date 2020/10/21 12:04
 * @Created by limeng
 */
object ProcessFunctionTimeOut {
  def main(args: Array[String]): Unit = {
    //构建运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint
    env.getCheckpointConfig
      .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,50000))

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "prd-dcr-01.xbox:9092,prd-data-01.xbox:9092")
    props.setProperty("zookeeper.connect", "prd-hadoop-03.xbox:2181,prd-hadoop-04.xbox:2181,prd-hadoop-05.xbox:2181")
    props.setProperty("group.id", "group-test")

    val consumer = new FlinkKafkaConsumer[String]("fk_string_topic",new SimpleStringSchema(),props)
    consumer.setStartFromEarliest()
    consumer.assignTimestampsAndWatermarks(new CustomWatermarkExtractor) //设置自定义时间戳分配器和watermark发射器，也可以在后面的算子中设置



    import org.apache.flink.api.scala._
    env.addSource(consumer).map(m=>{
      (m,System.currentTimeMillis().toLong)
    })
      .keyBy(0)
      .process(new CountWithTimeoutFunction).print()


    env.execute("ProcessFunctionTimeOut")
  }

  class CountWithTimeoutFunction extends ProcessFunction[(String,Long),(String,Long)]{
    //保留处理函数内部的所有状态的一个并行度
    var state:ValueState[CountWithTimestamp] = _


    override def open(parameters: Configuration): Unit = {
      state = getRuntimeContext.getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))
    }

    /**
     * 针对每一个元素会调用该方法，根据相关逻辑去更改内部状态
     * @param value
     * @param ctx
     * @param out
     */
    override def processElement(value: (String, Long), ctx: ProcessFunction[(String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
      // 查看当前计数
      var current = state.value()

      if(current == null){
        current = new CountWithTimestamp()
        current.key = value._1
      }
      // 更新状态中的计数
      current.count += 1
      // 设置状态的时间戳为记录的事件时间时间戳
      current.lastModified = ctx.timestamp()

      //根据key进行状态更新
      state.update(current)

      //注册基于事件时间的timer
      ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)

      // 注册基于处理时间的timer
      ctx.timerService.registerProcessingTimeTimer(current.lastModified + 60000)

    }

    override def onTimer(timestamp: Long, ctx: ProcessFunction[(String, Long), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
      // 得到设置这个定时器的键对应的状态
      val result = state.value

      //println("onTimer : " + result.key)
      // 检查定时器是过时定时器还是最新定时器
      if (timestamp == result.lastModified + 1) {
        println("onTimer timeout : " + result.key)
        out.collect((result.key, result.count))
      }

    }
  }


  /**
   *  该自定义时间戳抽取器和触发器实际上是使用的注入时间，因为发的数据不带时间戳的。
   */
  class CustomWatermarkExtractor extends AssignerWithPeriodicWatermarks[String] {

    var currentTimestamp = Long.MinValue


    /**
     * wateMark生成器
     * @return
     */
    override def getCurrentWatermark: Watermark = {
      new Watermark(if (currentTimestamp == Long.MinValue)
        Long.MinValue
      else
        currentTimestamp - 1)
    }

    /**
     * 时间抽取
     * @param element
     * @param previousElementTimestamp
     * @return
     */
    override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
      currentTimestamp = System.currentTimeMillis()
      currentTimestamp
    }

  }
}
