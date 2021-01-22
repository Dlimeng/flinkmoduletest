package com.lm.flink.dataset.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
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
    val bsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val bsTableEnv  = StreamTableEnvironment.create(bsEnv,bsSettings)


    val name = "myhive"
    val defaultDatabase = "linkis_db"
    val hiveConfDir = "/etc/hive/conf"
    val version = "1.1.0"

    val hiveCatalog = new HiveCatalog(name,defaultDatabase,hiveConfDir,version)

    bsTableEnv.registerCatalog(name,hiveCatalog)
    bsTableEnv.useCatalog(name)

    val sqlTable = bsTableEnv.sqlQuery("select * from test1")

    import org.apache.flink.api.scala._

    val retracts = bsTableEnv.toRetractStream[TestModel](sqlTable)

    retracts.print()

    bsTableEnv.execute("HiveDemo")
  }

  case class TestModel(id:String) extends Serializable
}
