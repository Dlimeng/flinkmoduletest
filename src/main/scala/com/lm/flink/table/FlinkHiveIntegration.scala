package com.lm.flink.table

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * @Classname FlinkHiveIntegration
 * @Description TODO
 * @Date 2020/11/28 17:16
 * @Created by limeng
 */
object FlinkHiveIntegration {

  def main(args: Array[String]): Unit = {
      val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()

//    val tableEnv = TableEnvironment.create(settings)
//
//    val name:String = "myhive"
//    val defaultDatabase:String = "linkis_db"
//    val hiveConfDir:String = "/etc/hive/conf"
//    val version:String = "1.1.0"
//
//    val hive = new HiveCatalog(name,defaultDatabase,hiveConfDir,version)
//
//    tableEnv.registerCatalog(name,hive)
//
//    tableEnv.useCatalog(name)
//
//
//    val table = tableEnv.sqlQuery("select id from test1")
//
//
//    table.insertInto("linkis_db.test1")
  }

  case class WcTest(id:String) extends Serializable
}
