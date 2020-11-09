package com.lm.flink.table

import com.lm.flink.datastream.source.OrderSourceFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
/**
 * @Classname StreamTableEnvironmentTest
 * @Description TODO
 * @Date 2020/11/9 18:34
 * @Created by limeng
 */
object StreamTableEnvironmentTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    import org.apache.flink.api.scala._
    val dataStream: DataStream[(Long, String, Int, Long)] = env.addSource(new OrderSourceFunction(10, 0.01f, 60, 0))

    dataStream.print()


  }
}
