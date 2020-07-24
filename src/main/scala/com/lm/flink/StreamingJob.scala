package com.lm.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import scala.collection.mutable

/**
 * @Classname StreamingJob
 * @Description TODO
 * @Date 2020/7/24 20:19
 * @Created by limeng
 */
object StreamingJob {
  def main(args: Array[String]): Unit = {
    //设定流环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.socketTextStream("localhost", 19999)


    //对读取的每一行文本
    val counts = text.flatMap(_.toLowerCase
      .split("\\W+")
      .filter(_.nonEmpty)) .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    // 将计算结果打印输出
    counts.print()

    // 开始执行流程序
    env.execute("Window Stream WordCount")

  }
}
