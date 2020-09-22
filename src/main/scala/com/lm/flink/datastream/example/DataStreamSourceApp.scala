package com.lm.flink.datastream.example

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.scala.{DataStream}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
/**
 * @Classname DataStreamSourceApp
 * @Description TODO
 * @Date 2020/9/21 19:19
 * @Created by limeng
 */
object DataStreamSourceApp {
  def main(args: Array[String]): Unit = {
   val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//   // socketStream(env)
//    parallelSourceFunction(env)
    env.execute("DataStreamSourceApp")
  }

  def richParallelSourceFunction(env:StreamExecutionEnvironment) ={
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomRichParallelSourceFunction).setParallelism(2)
    data.print()
  }

  /**
   * 设置并行度 自定义source
   * @param env
   * @return
   */
  def parallelSourceFunction(env:StreamExecutionEnvironment) ={
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(2)
    data.print()
  }

  /**
   * 自定义source 不能并行处理
   *
   * @param env
   */
  def nonParallelSourceFunction(env: StreamExecutionEnvironment) = {

    import org.apache.flink.api.scala._
    //data不能设置大于1的并行度
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print()
  }

  /**
   * socket 简单流处理
   *
   * @param env
   */
  def socketStream(env: StreamExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val textStream: DataStream[String] = env.socketTextStream("localhost", 9999)

    textStream.print().setParallelism(1)
  }
}
