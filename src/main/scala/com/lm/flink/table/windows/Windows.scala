package com.lm.flink.table.windows


import com.lm.flink.datastream.source.OrderSourceFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
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



    val dataStream = env.addSource(OrderSourceFunction.apply())

    dataStream.print("dataStream")

    val table = dataStream.toTable(tableEnv,'user, 'product, 'amount,'times.rowtime )


    tableEnv.createTemporaryView("test",table)

    val table1 = tableEnv.sqlQuery("select user as wStart from test GROUP BY tumble(times, INTERVAL '5' SECOND),user")

    table1.toRetractStream[(Long, String, Int, Long)].print()

    env.execute("Windows")
  }
}
