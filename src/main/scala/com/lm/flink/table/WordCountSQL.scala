package com.lm.flink.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment

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

    val lines: DataSet[(String, Int)] = env.fromCollection(words.map(m=> (m,1)))

    tEnv.createTemporaryView("word",lines)

    val table = tEnv.sqlQuery("select * from word")

    tEnv.toDataSet(table)


    tEnv.execute("java_job");


  }

}
