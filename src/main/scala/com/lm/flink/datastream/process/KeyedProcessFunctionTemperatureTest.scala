package com.lm.flink.datastream.process

import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Classname KeyedProcessFunctionTes
 * @Description TODO
 * @Date 2020/10/21 17:00
 * @Created by limeng
 *  温度监控
 */
object KeyedProcessFunctionTemperatureTest {
  def main(args: Array[String]): Unit = {
     val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("192.168.200.116",9999)

    import org.apache.flink.api.scala._
    val dataDstream = stream.map(data=>{
      val arr = data.split(",")
      Record(arr(0),arr(1).trim.toLong,arr(2).trim.toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Record](Time.seconds(1)){
      override def extractTimestamp(element: Record): Long = {
        element.timestamp * 1000
      }
    })

    val resultDStrem = dataDstream.keyBy(_.id).process(new TempIncreaseAlertFunction())


    dataDstream.print("data")
    resultDStrem.print("result")

     env.execute("KeyedProcessFunctionTemperatureTest")
  }

  /**
   * 如果某传感器的温度在1秒（处理时间）持续增加
   * 则发出警告
   */
  class TempIncreaseAlertFunction extends  KeyedProcessFunction[String, Record, String] {
    import org.apache.flink.api.scala._
    //定义一个值状态，保存上一个设备温度值
    lazy val lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp",Types.of[Double]))
    //保存注册的定时器的时间戳
    lazy val currentTimer = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer",Types.of[Long]))


    override def processElement(value: Record, ctx: KeyedProcessFunction[String, Record, String]#Context, out: Collector[String]): Unit = {
      //获取前一个温度
      val prevTemp = lastTemp.value()
      //更新最近一次温度
      lastTemp.update(value.temperature)

      //获取上一个定时器的时间戳
      val curTimerTimestamp =  currentTimer.value()

      if(prevTemp == 0.0 || value.temperature < prevTemp) {
          //温度下降，删除当前计时器
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        currentTimer.clear()
      }else if(value.temperature > prevTemp && curTimerTimestamp == 0){
        //温度上升，没有设置计时器
        //以当前时间 +1秒设置处理时间计时器
        val timerTs = ctx.timerService().currentProcessingTime() + 1000
        ctx.timerService().registerProcessingTimeTimer(timerTs)
        //更新当前计时器
        currentTimer.update(timerTs)
      }

    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Record, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("设备 id 为: " + ctx.getCurrentKey + "的设备温度值已经连续 1s 上升了。")
      currentTimer.clear()
    }

  }

}

case class Record(id:String,timestamp:Long,temperature:Double) extends Serializable
