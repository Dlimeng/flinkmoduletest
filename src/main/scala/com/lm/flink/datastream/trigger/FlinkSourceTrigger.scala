package com.lm.flink.datastream.trigger

import com.lm.flink.datastream.source.SensorSource
import com.lm.flink.model.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousProcessingTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Classname FlinkSourceTrigger
 * @Description TODO
 * @Date 2021/1/12 18:29
 * @Created by limeng
 */
class FlinkSourceTrigger {

}
object FlinkSourceTrigger{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(5000)
    //每9秒发出一个watermark
   // env.getConfig.setAutoWatermarkInterval(9000)
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    import org.apache.flink.api.scala._

     env.addSource(new SensorSource).keyBy(f=>f.id)
       .timeWindow(Time.milliseconds(10))
      //.trigger(ContinuousProcessingTimeTrigger.of(Time.milliseconds(20)))
       .process(new ProcessWindowFunction[SensorReading,SensorReading,String,TimeWindow] {
       override def process(key: String, context: Context, elements: Iterable[SensorReading], out: Collector[SensorReading]): Unit = {
         println(s"start size:${elements.size}")
         elements.foreach(f=>{
           out.collect(f)
         })
         println(s"end")
       }
     }).print()



    env.execute("FlinkSourceTrigger")
  }
}
