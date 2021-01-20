package com.lm.flink.dataset.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * @Classname HiveDemo
 * @Description TODO
 * @Date 2021/1/20 15:41
 * @Created by limeng
 */
class HiveDemo {

}
object HiveDemo{
  def main(args: Array[String]): Unit = {
    val fbEnv = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(fbEnv)

    val name = "myhive"
    val defaultDatabase = "linkis_db"
    val hiveConfDir = "/etc/hive/conf"
    val version = "1.1.0"

    val hiveCatalog = new HiveCatalog(name,defaultDatabase,hiveConfDir,version)

    tableEnv.registerCatalog(name,hiveCatalog)
    tableEnv.useCatalog(name)

    val sqlTable = tableEnv.sqlQuery("select * from test1")

    import org.apache.flink.api.scala._
    val toDataSet = tableEnv.toDataSet[TestModel](sqlTable)

    toDataSet.print()
  }

  case class TestModel(id:String) extends Serializable
}
