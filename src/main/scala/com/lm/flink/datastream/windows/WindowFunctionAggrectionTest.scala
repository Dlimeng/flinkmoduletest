package com.lm.flink.datastream.windows

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * @Classname WindowFunctionAggrectionTest
 * @Description TODO
 * @Date 2020/11/22 17:26
 * @Created by limeng
 * 从SocketSource接收数据，时间语义采用ProcessingTime，通过Flink 时间窗口以及aggregate方法统计用户在24小时内的平均消费金额。
 */
object WindowFunctionAggrectionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setParallelism(1)

    val socketData = env.socketTextStream("localhost",9999)
    socketData.print("input: ")

    import org.apache.flink.api.scala._

    socketData.map(line=>{
      ConsumerMess(line.split(",")(0).toInt, line.split(",")(1).toDouble)
    }).keyBy(_.userId)
      .timeWindow(Time.seconds(3))
      .aggregate(new MyAggregrateFunction)
      .print("output ")

    env.execute("WindowFunctionAggrectionTest")
  }

  case  class ConsumerMess(userId:Int, spend:Double)


  class MyAggregrateFunction extends AggregateFunction[ConsumerMess, (Int, Double), Double]{
    override def createAccumulator(): (Int, Double) = (0,0)

    override def add(value: ConsumerMess, accumulator: (Int, Double)): (Int, Double) = {
      (accumulator._1 + 1,accumulator._2+value.spend)
    }

    override def getResult(accumulator: (Int, Double)): Double = {
      accumulator._2 / accumulator._1
    }

    override def merge(a: (Int, Double), b: (Int, Double)): (Int, Double) = {
      (a._1+b._1 ,b._2 + a._2)
    }
  }
}
