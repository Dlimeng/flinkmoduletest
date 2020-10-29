package com.lm.flink.datastream.join

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Classname IntervalJoin
 * @Description TODO
 * @Date 2020/10/27 20:32
 * @Created by limeng
 *  区间关联当前仅支持EventTime
 *  Interval JOIN 相对于UnBounded的双流JOIN来说是Bounded JOIN。就是每条流的每一条数据会与另一条流上的不同时间区域的数据进行JOIN。
 */
object IntervalJoin {

  def main(args: Array[String]): Unit = {

    //设置至少一次或仅此一次语义
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置至少一次或仅此一次语义
    env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE)

    //设置
    env.getCheckpointConfig
      .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,50000))

    env.setParallelism(1)

    val dataStream1 = env.socketTextStream("localhost",9999)
    val dataStream2 = env.socketTextStream("localhost",9998)

    import org.apache.flink.api.scala._
    val dataStreamMap1 = dataStream1.map(f=>{
      val tokens = f.split(",")
      StockTransaction(tokens(0),tokens(1),tokens(2).toDouble)
    }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[StockTransaction]{
      var currentTimestamp = 0L

      val maxOutOfOrderness = 1000L

      override def getCurrentWatermark: Watermark = {
        val tmpTimestamp = currentTimestamp - maxOutOfOrderness
        println(s"wall clock is ${System.currentTimeMillis()}  new watermark ${tmpTimestamp}")
        new Watermark(tmpTimestamp)
      }

      override def extractTimestamp(element: StockTransaction, previousElementTimestamp: Long): Long = {
        val timestamp  = element.txTime.toLong
        currentTimestamp = Math.max(timestamp,currentTimestamp)
        println(s"get timestamp is $timestamp currentMaxTimestamp $currentTimestamp")
        currentTimestamp
      }
    })


    val dataStreamMap2 = dataStream2.map(f=>{
      val tokens = f.split(",")
      StockSnapshot(tokens(0),tokens(1),tokens(2).toDouble)
    }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[StockSnapshot]{
      var currentTimestamp = 0L

      val maxOutOfOrderness = 1000L

      override def getCurrentWatermark: Watermark = {
        val tmpTimestamp = currentTimestamp - maxOutOfOrderness
        println(s"wall clock is ${System.currentTimeMillis()}  new watermark ${tmpTimestamp}")
        new Watermark(tmpTimestamp)
      }

      override def extractTimestamp(element: StockSnapshot, previousElementTimestamp: Long): Long = {
        val timestamp  = element.mdTime.toLong
        currentTimestamp = Math.max(timestamp,currentTimestamp)
        println(s"get timestamp is $timestamp currentMaxTimestamp $currentTimestamp")
        currentTimestamp
      }
    })

    dataStreamMap1.print("dataStreamMap1")
    dataStreamMap2.print("dataStreamMap2")

    dataStreamMap1.keyBy(_.txCode)
      .intervalJoin(dataStreamMap2.keyBy(_.mdCode))
      .between(Time.minutes(-10),Time.seconds(0))
      .process(new ProcessJoinFunction[StockTransaction,StockSnapshot,String] {
        override def processElement(left: StockTransaction, right: StockSnapshot, ctx: ProcessJoinFunction[StockTransaction, StockSnapshot, String]#Context, out: Collector[String]): Unit = {
          out.collect(left.toString +" =Interval Join=> "+right.toString)
        }
      }).print()



    env.execute("IntervalJoin")

  }
  case class StockTransaction(txTime:String,txCode:String,txValue:Double) extends Serializable{
    override def toString: String = txTime +"#"+txCode+"#"+txValue
  }

  case class StockSnapshot(mdTime:String,mdCode:String,mdValue:Double) extends Serializable {
    override def toString: String = mdTime +"#"+mdCode+"#"+mdValue
  }


}


