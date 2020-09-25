package com.lm.flink.datastream.windows

import com.lm.flink.datastream.example.CustomParallelSourceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Classname TumblingWindowsProcessFunction
 * @Description TODO
 * @Date 2020/9/25 13:51
 * @Created by limeng
 */
object TumblingWindowsProcessFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getConfig.setAutoWatermarkInterval(1000) //watermark时间间隔

    /**
     * 绝大部分业务使用eventTime ,一般只在eventTime无法使用时才会被迫使用 ProcessingTime 或者 IngestionTime
     * 从调用时刻开始给env 创建的每一个stream追加时间特征
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    /**
     * Flink 将Window分为两类，一类叫做Keyed window ,另一类叫做Non-Keyed window
     */
    import org.apache.flink.api.scala._
    val process  = env.addSource(new CustomParallelSourceFunction()).map(m=>{
      (m,1)
    })
      .keyBy(_._1)
      .timeWindow(Time.milliseconds(10)) //滚动窗口,固定窗口
      .process(new MyProcessWindowFunction)


    process.print()
    env.execute("TumblingWindowsProcessFunction")
  }

  /**
   *  获取包含窗口的所有数据元的Iterable，以及可访问时间和状态信息的Context对象，这使其能够提供比其他窗口函数更多的灵活性。
   *  这是以性能和资源消耗为代价的，因为数据元不能以递增方式聚合，而是需要在内部进行缓存，直到窗口被认为已准备好进行处理
   *
   *   ProcessWindowFunction可以与ReduceFunction，AggregateFunction或FoldFunction以递增地聚合数据元。
   *   当窗口关闭时，ProcessWindowFunction将提供聚合结果。这允许它在访问附加窗口元信息的同时递增地计算窗口ProcessWindowFunction。
   */
  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Int), String, String, TimeWindow]{

    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
      var count:Int = 0
      //ProcessWindowFunction 对窗口中的数据进行计数的情况。
      for(in <- elements){
        count = count+1
      }
      context.window
      out.collect(s"Window ${context.window} count: $count")
    }
  }



}

