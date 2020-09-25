package com.lm.flink.datastream.example


import com.lm.flink.model.SourceBean
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
    import org.apache.flink.api.scala._
    val data = env.addSource(new MySQLRichParallelSourceFunction)
    val stream = data.map(m=>{
      val id = m.id +"1"
      SourceBean(id,m.name,m.age,m.ctime)
    })
    stream.addSink(new RichSinkFunctionToMySQL)
    env.execute("DataStreamSinkToMysqlApp")
  }
}
