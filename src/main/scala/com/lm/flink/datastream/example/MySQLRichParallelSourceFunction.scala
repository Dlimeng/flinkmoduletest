package com.lm.flink.datastream.example

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.lm.flink.model.SourceBean
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}

/**
 * @Classname MyRichParallelSourceFunction
 * @Description TODO
 * @Date 2020/9/21 15:30
 * @Created by limeng
 */
class MySQLRichParallelSourceFunction extends RichSourceFunction[SourceBean] {

  var isRUNNING: Boolean = true
  var ps: PreparedStatement = null
  var conn: Connection = null

  /**
   * 建立连接
   */
  def getConnection():Connection = {
    var conn:Connection = null
    val url:String = "jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false"
    val user: String = "root"
    val password: String = "root"

    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      conn = DriverManager.getConnection(url, user, password)
    }catch {
      case _: Throwable => println("due to the connect error then exit!")
    }
    conn
  }


  override def run(sourceContext: SourceFunction.SourceContext[SourceBean]): Unit = {
    val resSet:ResultSet = ps.executeQuery()
    while(isRUNNING & resSet.next()) {
      sourceContext.collect(SourceBean(resSet.getString("id"),resSet.getString("name"),resSet.getInt("age"),resSet.getDate("ctime")))
    }
  }

  override def cancel(): Unit = {
    isRUNNING = false
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = this.getConnection()
    val sql = "select * from soure1"
    ps = this.conn.prepareStatement(sql)
  }

  override def close(): Unit = {
      if(conn != null){
        conn.close()
      }

      if(ps != null){
        ps.close()
      }
  }

}
