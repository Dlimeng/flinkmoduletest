package com.lm.flink.datastream.join

import java.lang
import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Classname InnerLeftRightJoinTest
 * @Description TODO
 * @Date 2020/10/26 17:22
 * @Created by limeng
 * window join
 */
object InnerLeftRightJoinTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //每9秒发出一个watermark
    env.setParallelism(1)
    env.getConfig.setAutoWatermarkInterval(9000)


    val dataStream1 = env.socketTextStream("localhost", 9999)
    val dataStream2 = env.socketTextStream("localhost", 9998)


    /**
     * operator操作
     * 数据格式：
     * tx:  2020/10/26 18:42:22,000002,10.2
     * md:  2020/10/26 18:42:22,000002,10.2
     *
     * 这里由于是测试，固水位线采用升序（即数据的Event Time 本身是升序输入）
     */
    import org.apache.flink.api.scala._
    val dataStreamMap1 = dataStream1
      .map(f => {
        val tokens = f.split(",")
        StockTransaction(tokens(0), tokens(1), tokens(2).toDouble)
      }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[StockTransaction] {
      var currentTimestamp = 0L

      val maxOutOfOrderness = 1000L

      override def getCurrentWatermark: Watermark = {
        val tmpTimestamp = currentTimestamp - maxOutOfOrderness
        println(s"wall clock is ${System.currentTimeMillis()}  new watermark ${tmpTimestamp}")
        new Watermark(tmpTimestamp)
      }

      override def extractTimestamp(element: StockTransaction, previousElementTimestamp: Long): Long = {
        val timestamp = element.txTime.toLong
        currentTimestamp = Math.max(timestamp, currentTimestamp)
        println(s"get timestamp is $timestamp currentMaxTimestamp $currentTimestamp")
        currentTimestamp
      }
    })


    val dataStreamMap2 = dataStream2
      .map(f => {
        val tokens = f.split(",")
        StockSnapshot(tokens(0), tokens(1), tokens(2).toDouble)
      }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[StockSnapshot] {
      var currentTimestamp = 0L

      val maxOutOfOrderness = 1000L

      override def getCurrentWatermark: Watermark = {
        val tmpTimestamp = currentTimestamp - maxOutOfOrderness
        println(s"wall clock is ${System.currentTimeMillis()}  new watermark ${tmpTimestamp}")
        new Watermark(tmpTimestamp)
      }

      override def extractTimestamp(element: StockSnapshot, previousElementTimestamp: Long): Long = {
        val timestamp = element.mdTime.toLong
        currentTimestamp = Math.max(timestamp, currentTimestamp)
        println(s"get timestamp is $timestamp currentMaxTimestamp $currentTimestamp")
        currentTimestamp
      }
    })


    dataStreamMap1.print("dataStreamMap1")
    dataStreamMap2.print("dataStreamMap2")


    /**
     * Join操作
     * 限定范围是3秒钟的Event Time窗口
     */
    val joinedStream = dataStreamMap1.coGroup(dataStreamMap2)
      .where(_.txCode)
      .equalTo(_.mdCode)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))


    val innerJoinedStream = joinedStream.apply(new InnerJoinFunction)
    val leftJoinedStream = joinedStream.apply(new LeftJoinFunction)
    val rightJoinedStream = joinedStream.apply(new RightJoinFunction)

    innerJoinedStream.name("InnerJoinedStream").print()
    leftJoinedStream.name("LeftJoinedStream").print()
    rightJoinedStream.name("RightJoinedStream").print()

    env.execute("InnerLeftRightJoinTest")
  }


  class InnerJoinFunction extends CoGroupFunction[StockTransaction, StockSnapshot, (String, String, String, Double, Double, String)] {
    override def coGroup(first: lang.Iterable[StockTransaction], second: lang.Iterable[StockSnapshot], out: Collector[(String, String, String, Double, Double, String)]): Unit = {
      import scala.collection.JavaConverters._
      val scalaT1 = first.asScala.toList
      val scalaT2 = second.asScala.toList


      println(scalaT1.size)
      println(scalaT2.size)

      /**
       * Inner join 要比较的是同一个key下，同一个时间窗口内
       */
      if (scalaT1.nonEmpty && scalaT2.nonEmpty) {
        for (transaction <- scalaT1) {
          for (snapshot <- scalaT2) {
            out.collect(transaction.txCode, transaction.txTime, snapshot.mdTime, transaction.txValue, snapshot.mdValue, "Inner Join Test")
          }
        }
      }

    }
  }

  class LeftJoinFunction extends CoGroupFunction[StockTransaction, StockSnapshot, (String, String, String, Double, Double, String)] {
    override def coGroup(T1: java.lang.Iterable[StockTransaction], T2: java.lang.Iterable[StockSnapshot], out: Collector[(String, String, String, Double, Double, String)]): Unit = {
      /**
       * 将Java中的Iterable对象转换为Scala的Iterable
       * scala的集合操作效率高，简洁
       */
      import scala.collection.JavaConverters._
      val scalaT1 = T1.asScala.toList
      val scalaT2 = T2.asScala.toList

      /**
       * Left Join要比较的是同一个key下，同一个时间窗口内的数据
       */
      if (scalaT1.nonEmpty && scalaT2.isEmpty) {
        for (transaction <- scalaT1) {
          out.collect(transaction.txCode, transaction.txTime, "", transaction.txValue, 0, "Left Join Test")
        }
      }
    }
  }

  class RightJoinFunction extends CoGroupFunction[StockTransaction, StockSnapshot, (String, String, String, Double, Double, String)] {
    override def coGroup(T1: java.lang.Iterable[StockTransaction], T2: java.lang.Iterable[StockSnapshot], out: Collector[(String, String, String, Double, Double, String)]): Unit = {
      /**
       * 将Java中的Iterable对象转换为Scala的Iterable
       * scala的集合操作效率高，简洁
       */
      import scala.collection.JavaConverters._
      val scalaT1 = T1.asScala.toList
      val scalaT2 = T2.asScala.toList

      /**
       * Right Join要比较的是同一个key下，同一个时间窗口内的数据
       */
      if (scalaT1.isEmpty && scalaT2.nonEmpty) {
        for (snapshot <- scalaT2) {
          out.collect(snapshot.mdCode, "", snapshot.mdTime, 0, snapshot.mdValue, "Right Join Test")
        }
      }
    }
  }


  case class StockTransaction(txTime: String, txCode: String, txValue: Double)

  case class StockSnapshot(mdTime: String, mdCode: String, mdValue: Double)

}


