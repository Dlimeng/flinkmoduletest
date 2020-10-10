package com.lm.flink.watermark

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.StringUtils

/**
 * @Classname DataStreamDemo
 * @Description TODO
 * @Date 2020/9/29 14:37
 * @Created by limeng
 */
object DataStreamDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置时间分配器
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(1)
    //每9秒发出一个watermark
    env.getConfig.setAutoWatermarkInterval(9000)

    val line = env.socketTextStream("localhost",9999)

    import org.apache.flink.api.scala._
    val counts  = line
      .filter(f=> !StringUtils.isNullOrWhitespaceOnly(f))
      .map(new LineSplitter)
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Tuple3[String, Long, Integer]](){
        var currentMaxTimestamp = 0L
        //这个控制失序已经延迟的度量
        val maxOutOfOrderness = 10000L
        //获取Watermark
        override def getCurrentWatermark: Watermark = {
          val tmpTimestamp = currentMaxTimestamp - maxOutOfOrderness
          println(s"wall clock is ${System.currentTimeMillis()}  new watermark ${tmpTimestamp}")
          new Watermark(tmpTimestamp)
        }
        //获取EventTime
        override def extractTimestamp(element: (String, Long, Integer), previousElementTimestamp: Long): Long = {
          val timestamp  = element._2
          currentMaxTimestamp = Math.max(timestamp,currentMaxTimestamp)
          println(s"get timestamp is $timestamp currentMaxTimestamp $currentMaxTimestamp")
          timestamp
        }
      }).keyBy(0)
      .timeWindow(Time.seconds(20))
      .sum(2)


    counts.print()
    env.execute("WordCount")
  }


}

class LineSplitter extends MapFunction[String,Tuple3[String, Long, Integer]]{
  override def map(value: String): (String, Long, Integer) = {
    val arrays = value.toLowerCase.split("\\W+")
    new Tuple3[String, Long, Integer](arrays(0), arrays(1).toLong, 1)
  }
}
