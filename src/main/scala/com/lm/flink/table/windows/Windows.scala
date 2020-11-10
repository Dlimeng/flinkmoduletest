package com.lm.flink.table.windows


import com.lm.flink.datastream.source.OrderSourceFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
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

    import org.apache.flink.api.scala._

    val dataStream = env.addSource(OrderSourceFunction.apply()).map(new MapFunction[(Long, String, Int, Long),Row] {
      override def map(value: (Long, String, Int, Long)): Row = {
        val row1 = new Row(4)
        row1.setField(0, value._1)
        row1.setField(1, value._2)
        row1.setField(2, value._3)
        row1.setField(3,value._4)
        row1
      }
    })

    dataStream.print("dataStream")

    val table = tableEnv.fromDataStream[Row](dataStream)

    tableEnv.createTemporaryView("test",table)

    val table1 = tableEnv.sqlQuery("select * from test ")

    table1.toRetractStream[Row].print()

    env.execute("Windows")
  }
}
