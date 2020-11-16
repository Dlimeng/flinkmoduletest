package com.lm.flink.table

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._





/**
 * @Classname WordCountSQL
 * @Description TODO
 * @Date 2020/10/29 18:04
 * @Created by limeng
 *
 *   org/apache/flink/table/api/TableEnvironment.java
 *   org/apache/flink/table/api/java/BatchTableEnvironment.java
 *   org/apache/flink/table/api/scala/BatchTableEnvironment.scala 批处理场景，
 *   org/apache/flink/table/api/java/StreamTableEnvironment.java
 *   org/apache/flink/table/api/scala/StreamTableEnvironment.scala 流处理
 *
 *  print() 里面
 */
object WordCountSQL {

  def main(args: Array[String]): Unit = {



    val env = ExecutionEnvironment.getExecutionEnvironment

    val tEnv = BatchTableEnvironment.create(env)

    import org.apache.flink.api.scala._

    val wordStr:String = "Hello Flink Hello TOM"
    val words =  wordStr.split("\\W+")

    val lines: DataSet[WC] = env.fromCollection(words.map(m => WC(m, 1)))



    tEnv.fromDataSet(lines).groupBy("word").select('word,'frequency.sum).toDataSet[WC].print()

   // env.execute("java_job");
  }

  case class WC(word:String,frequency:Long) extends Serializable

}
