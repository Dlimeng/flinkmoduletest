package com.lm.flink.dataset.table

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment

/**
 * @Classname TableToDataSet
 * @Description TODO
 * @Date 2021/1/20 16:42
 * @Created by limeng
 */
object TableToDataSet {
  def main(args: Array[String]): Unit = {
    //构造数据，转换为table
    val data = List(
      Peoject(1L, 1, "Hello"),
      Peoject(2L, 2, "Hello"),
      Peoject(3L, 3, "Hello"),
      Peoject(4L, 4, "Hello"),
      Peoject(5L, 5, "Hello"),
      Peoject(6L, 6, "Hello"),
      Peoject(7L, 7, "Hello World"),
      Peoject(8L, 8, "Hello World"),
      Peoject(8L, 8, "Hello World"),
      Peoject(20L, 20, "Hello World"))
    //初始化环境，加载table数据
    val fbEnv = ExecutionEnvironment.getExecutionEnvironment
    val fbTableEnv = BatchTableEnvironment.create(fbEnv)

    import org.apache.flink.api.scala._
    val collection: DataSet[Peoject] = fbEnv.fromCollection(data)
    val table: Table = fbTableEnv.fromDataSet(collection)
    //TODO 将table转换为dataSet
    val toDataSet: DataSet[Peoject] = fbTableEnv.toDataSet[Peoject](table)
    toDataSet.print()

  }
  case class Peoject(user: Long, index: Int, content: String)
}
