package com.lm.flink.table.windows


import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
/**
 * @Classname Windows
 * @Description TODO
 * @Date 2020/11/9 20:00
 * @Created by limeng
 */
object Windows {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)


    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(4000)
    env.getConfig.setAutoWatermarkInterval(1000)



  }
}
