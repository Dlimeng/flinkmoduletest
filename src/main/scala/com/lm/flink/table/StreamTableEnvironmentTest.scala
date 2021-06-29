package com.lm.flink.table

import com.lm.flink.datastream.source.OrderSourceFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

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


//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.enableCheckpointing(4000)
//    env.getConfig.setAutoWatermarkInterval(1000)

    import org.apache.flink.api.scala._
    val dataStream: DataStream[(Long, String, Int, Long)] = env.addSource(OrderSourceFunction.apply())
    dataStream.print("dataStream")

    val table = tableEnv.fromDataStream(dataStream)

    tableEnv.createTemporaryView("test",table)

    val table1 = tableEnv.sqlQuery("select * from test")


    table1.toRetractStream[(Long, String, Int, Long)].print()

    env.execute("StreamTableEnvironmentTest")
  }
}
