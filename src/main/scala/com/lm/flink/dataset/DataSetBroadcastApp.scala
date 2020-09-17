package com.lm.flink.dataset

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

/**
 * @Classname DataSetBroadcastApp
 * @Description TODO
 * @Date 2020/9/16 20:36
 * @Created by limeng
 * flink 支持广播变量，就是将数据广播到具体taskManager上，数据存储在内存中，这样可以减缓大量的shuffle操作
 *
 */
object DataSetBroadcastApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val toBroadcast  = env.fromElements(1,2,3)

    val data = env.fromElements("a","b")
    /**
     * RichMapFunction 富函数上下文信息
     */
    val result = data.map(new RichMapFunction[String,String](){
      var mList: mutable.Buffer[String] = _
      override def open(config: Configuration): Unit = {
        import scala.collection.JavaConverters._
        mList  = getRuntimeContext().getBroadcastVariable[String]("broadcastSetName").asScala
      }

      override def map(in: String): String = {
        in +"--->广播数据"+mList.toString()
      }
    }).withBroadcastSet(toBroadcast,"broadcastSetName")

    result.print()
  }
}
