package com.lm.flink.datastream.sideoutputs


import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

/**
 * @Classname SideOutputTest2
 * @Description TODO
 * @Date 2020/10/19 17:15
 * @Created by limeng
 */
object SideOutputTest2 {
  import org.apache.flink.api.scala._
  val webTerminal: OutputTag[MdMsg] = new OutputTag[MdMsg]("Web端埋点数据")
  val mobileTerminal: OutputTag[MdMsg] = new OutputTag[MdMsg]("移动端埋点数据")
  val csTerminal: OutputTag[MdMsg] = new OutputTag[MdMsg]("CS端埋点数据")

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    env.setStateBackend(new MemoryStateBackend(100, false))

    val socketData: DataStream[String] = env.socketTextStream("localhost", 9999)
    socketData.print("input data")

    val outputStream: DataStream[MdMsg] = socketData.map(line => {
      val str: Array[String] = line.split(",")
      MdMsg(str(0), str(1), str(2).toLong)
    })
      .process(new ProcessFunction[MdMsg,MdMsg]{
        override def processElement(value: MdMsg, ctx: ProcessFunction[MdMsg, MdMsg]#Context, out: Collector[MdMsg]): Unit = {
          // web
          if (value.mdType == "web") {
            ctx.output(webTerminal, value)
            // mobile
          } else if (value.mdType == "mobile") {
            ctx.output(mobileTerminal, value)
            // cs
          } else if (value.mdType == "cs") {
            ctx.output(csTerminal, value)
            // others
          } else {
            out.collect(value)
          }
        }
      })

    // Web端埋点数据流处理逻辑
    outputStream.getSideOutput(webTerminal).print("web")
    // Mobile端埋点数据流处理逻辑
    outputStream.getSideOutput(mobileTerminal).print("mobile")
    // CS端埋点数据流处理逻辑
    outputStream.getSideOutput(csTerminal).print("cs")

    env.execute()

  }

  case class MdMsg(mdType:String, url:String, Time:Long)


}
