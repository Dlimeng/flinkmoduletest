package com.lm.flink.datastream.trigger

import com.lm.flink.datastream.source.SensorSource
import com.lm.flink.datastream.trigger.FlinkSourceTrigger.model
import com.lm.flink.model.SensorReading
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousProcessingTimeTrigger, Trigger}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, StringUtils}

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

  //  env.enableCheckpointing(5000)
    //每9秒发出一个watermark
   // env.getConfig.setAutoWatermarkInterval(9000)
    env.setParallelism(1)

   // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val line = env.socketTextStream("192.168.200.116",9999)

    import org.apache.flink.api.scala._

    line.filter(f=> !StringUtils.isNullOrWhitespaceOnly(f))
        .map(new LineSplitter)
        .keyBy(f=>f.id)
        .timeWindow(Time.seconds(10))
        .trigger(new CountTrigger(2))
        .process(new MyProcessWindowFunction)
        .print()




    env.execute("FlinkSourceTrigger")
  }

  case class model(id:String,time:Long,num:Int) extends Serializable

  class MyProcessWindowFunction extends ProcessWindowFunction[model, model, String, TimeWindow]{

    override def process(key: String, context: Context, elements: Iterable[model], out: Collector[model]): Unit = {
      println("start")
      elements.foreach(f=>{
        out.collect(f)
      })
      println("end")
    }
  }
  class LineSplitter extends MapFunction[String,model]{
    override def map(value: String): model = {
      val arrays = value.toLowerCase.split("\\W+")
      model(arrays(0), arrays(1).toLong, 1)
    }
  }
}


