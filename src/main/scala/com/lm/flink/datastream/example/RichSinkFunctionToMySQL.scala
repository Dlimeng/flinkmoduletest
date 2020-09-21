package com.lm.flink.datastream.example

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import com.lm.flink.model.SourceBean
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

/**
 * @Classname RichSinkFunctionToMySQL
 * @Description TODO
 * @Date 2020/9/21 16:09
 * @Created by limeng
 */
class RichSinkFunctionToMySQL extends RichSinkFunction[SourceBean]{
  var isRUNNING: Boolean = true
  var ps: PreparedStatement = null
  var conn: Connection = null

  // 建立连接
  def getConnection():Connection = {
    var conn: Connection = null
    val url: String = "jdbc:mysql://192.168.200.115:3306/kd_test?useSSL=false"
    val user: String = "root"
    val password: String = "root"

    try{
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection(url, user, password)
    } catch {
      case _: Throwable => println("due to the connect error then exit!")
    }
    conn
  }

  /**
   * open()初始化建立和 MySQL 的连接
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = getConnection()
    val sql: String = "insert into soure1(id, name , age,ctime) values(?, ?, ?, ?);"
    ps = this.conn.prepareStatement(sql)
  }

  /**
   * 组装数据，进行数据的插入操作
   * 对每条数据的插入都要调用invoke()方法
   *
   * @param value
   */
  override def invoke(value: SourceBean): Unit = {
    ps.setString(1, value.id)
    ps.setString(2, value.name)
    ps.setInt(3, value.age)
    ps.setDate(4, new Date(value.ctime.getTime))
    ps.executeUpdate()
  }

  override def close(): Unit = {
    if (conn != null) {
      conn.close()
    }
    if(ps != null) {
      ps.close()
    }
  }

}
