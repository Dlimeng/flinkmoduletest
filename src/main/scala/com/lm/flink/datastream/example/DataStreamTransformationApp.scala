package com.lm.flink.datastream.example

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{SplitStream, StreamExecutionEnvironment}

/**
 * @Classname DataStreamTransformationApp
 * @Description TODO
 * @Date 2020/9/21 20:03
 * @Created by limeng
 */
object DataStreamTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    import org.apache.flink.api.scala._

    //splitSelectFunction(env)
    //unionFunction(env)


    env.execute("DataStreamTransformationApp")
  }

  /**
   * dataStream 流处理 split和select算子结合使用
   *
   * @param env
   */
  def splitSelectFunction(env:StreamExecutionEnvironment): Unit ={
    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomNonParallelSourceFunction)

    val splits: SplitStream[Long] = data.split(new OutputSelector[Long] {
      override def select(value: Long): java.lang.Iterable[String] = {
        val list = new java.util.ArrayList[String]()
        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("odd")
        }
        list
      }
    })
    //可以传多个
    splits.select("even").print().setParallelism(1)
  }


  /**
   * dataStream 流处理 union算子
   *
   * @param env
   */
  def unionFunction(env: StreamExecutionEnvironment) = {
    import org.apache.flink.api.scala._
    val data1 = env.addSource(new CustomNonParallelSourceFunction)
    val data2 = env.addSource(new CustomNonParallelSourceFunction)

    data1.union(data2).print().setParallelism(1)


  }

  /**
   * dataStream 流处理 filter算子
   *
   * @param env
   */
  def filterFunction(env: StreamExecutionEnvironment): Unit = {

    import org.apache.flink.api.scala._
    val data = env.addSource(new CustomNonParallelSourceFunction)

    data.map(x => x).filter(_ % 2 == 0).print().setParallelism(1)

  }

}
