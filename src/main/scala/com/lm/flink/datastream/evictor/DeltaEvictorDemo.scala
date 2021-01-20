package com.lm.flink.datastream.evictor

import com.lm.flink.datastream.trigger.CountTrigger
import com.lm.flink.datastream.trigger.FlinkSourceTrigger.{LineSplitter, MyProcessWindowFunction}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.evictors.DeltaEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, StringUtils}

/**
 * @Classname DeltaEvictorDemo
 * @Description TODO
 * @Date 2021/1/20 14:39
 * @Created by limeng
 */
class DeltaEvictorDemo {

}
object DeltaEvictorDemo{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置时间分配器
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(1)
    //每9秒发出一个watermark
    env.getConfig.setAutoWatermarkInterval(9000)

    val line = env.socketTextStream("192.168.200.116",9999)

    import org.apache.flink.api.scala._

    line.filter(f=> !StringUtils.isNullOrWhitespaceOnly(f))
      .map(m=>{
        val arrays = m.toLowerCase.split("\\W+")
        model(arrays(0), arrays(1).toLong, arrays(2).toDouble)
      }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[model](){
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
      override def extractTimestamp(element: model, previousElementTimestamp: Long): Long = {
        val timestamp  = element.time
        currentMaxTimestamp = Math.max(timestamp,currentMaxTimestamp)
        println(s"get timestamp is $timestamp currentMaxTimestamp $currentMaxTimestamp")
        timestamp
      }
      })
      .keyBy(f=>f.id)
      .timeWindow(Time.seconds(20))
      .evictor(DeltaEvictor.of(10.0,new DeltaFunction[model] {
        override def getDelta(oldDataPoint: model, newDataPoint: model): Double = {
          newDataPoint.productPrice - oldDataPoint.productPrice
        }
      }))
      .process(new MyProcessWindowFunction)
      .print()



    env.execute("DeltaEvictorDemo")
  }

  case class model(id:String,time:Long,productPrice:Double) extends Serializable

  class MyProcessWindowFunction extends ProcessWindowFunction[model, model, String, TimeWindow]{

    override def process(key: String, context: Context, elements: Iterable[model], out: Collector[model]): Unit = {
      println("start")
      elements.foreach(f=>{
        out.collect(f)
      })
      println("end")
    }
  }

}
