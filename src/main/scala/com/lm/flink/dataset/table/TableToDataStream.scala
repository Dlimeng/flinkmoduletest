package com.lm.flink.dataset.table

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

/**
 * @Classname TableToDataStream
 * @Description TODO
 * @Date 2021/1/20 16:29
 * @Created by limeng
 */
object TableToDataStream {
  def main(args: Array[String]): Unit = {
    //构造数据，转换为table
    val data = List(
      Peoject(1L, 1, "Hello"),
      Peoject(2L, 2, "Hello"),
      Peoject(3L, 3, "Hello"),
      Peoject(4L, 4, "Hello"),
      Peoject(5L, 5, "Hello"),
      Peoject(6L, 6, "Hello"),
      Peoject(7L, 7, "Hello World"),
      Peoject(8L, 8, "Hello World"),
      Peoject(8L, 8, "Hello World"),
      Peoject(20L, 20, "Hello World"))

    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(bsEnv, bsSettings)

    import org.apache.flink.api.scala._

    val stream = bsEnv.fromCollection(data)
    val table = tEnv.fromDataStream(stream)
    //TODO 将table转换为DataStream----将一个表附加到流上Append Mode
    val appendStream: DataStream[Peoject] = tEnv.toAppendStream[Peoject](table)
    //TODO 将表转换为流Retract Mode true代表添加消息，false代表撤销消息
    val retractStream: DataStream[(Boolean, Peoject)] = tEnv.toRetractStream[Peoject](table)
    retractStream.print()

    bsEnv.execute()
  }
  case class Peoject(user: Long, index: Int, content: String)
}
