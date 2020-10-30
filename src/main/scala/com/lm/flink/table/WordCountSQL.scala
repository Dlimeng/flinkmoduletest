package com.lm.flink.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.BatchTableDescriptor

import scala.collection.mutable.ListBuffer


/**
 * @Classname WordCountSQL
 * @Description TODO
 * @Date 2020/10/29 18:04
 * @Created by limeng
 */
object WordCountSQL {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val tEnv = BatchTableEnvironment.create(env)

    import org.apache.flink.api.scala._

    val wordStr:String = "Hello Flink Hello TOM"
    val words =  wordStr.split("\\W+")

    val lines = env.fromCollection(words.map(m=> WC(m,1)))

    val table = tEnv.fromDataSet(lines);

    tEnv.createTemporaryView("WordCount",table)


    val table1: Table = tEnv.sqlQuery("select * from WordCount")






    env.execute("java_job");
  }

  case class WC(word:String,frequency:Long) extends Serializable

}
