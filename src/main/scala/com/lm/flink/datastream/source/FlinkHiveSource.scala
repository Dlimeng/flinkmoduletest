package com.lm.flink.datastream.source


import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
/**
 * @Classname FlinkHiveSource
 * @Description TODO
 * @Date 2020/11/16 18:49
 * @Created by limeng
 */
object FlinkHiveSource {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val tableEnv = BatchTableEnvironment.create(env)

    import org.apache.flink.api.scala._

    val name            = "myhive"
    val defaultDatabase = "linkis_db"
    val hiveConfDir     = "E:/hive" // a local path
    val version         = "1.1.10"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version)
    tableEnv.registerCatalog("myhive",hive)
    tableEnv.useCatalog("myhive")

    val table = tableEnv.sqlQuery("select * from linkis_db.test1")


    table.printSchema()



  }
}
