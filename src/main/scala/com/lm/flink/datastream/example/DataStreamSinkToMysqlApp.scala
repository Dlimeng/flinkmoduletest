package com.lm.flink.datastream.example

import java.util.Date

import com.lm.flink.model.SourceBean
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Classname DataStreamSinkToMysqlApp
 * @Description TODO
 * @Date 2020/9/21 16:29
 * @Created by limeng
 *
 */
object DataStreamSinkToMysqlApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost",9999)

    //text.print()
    import org.apache.flink.api.scala._
    val personStream = text.map(new MapFunction[String, SourceBean] {
      override def map(value: String): SourceBean = {
        val spilt = value.split(",")
        SourceBean(spilt(0).toString, spilt(1).toString, Integer.parseInt(spilt(2)),new Date())
      }
    })
    personStream.addSink(new RichSinkFunctionToMySQL)


    env.execute("DataStreamSinkToMysqlApp")

  }
}
