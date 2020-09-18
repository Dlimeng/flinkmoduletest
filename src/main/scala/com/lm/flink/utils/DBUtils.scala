package com.lm.flink.utils

import scala.util.Random

/**
 * @Classname DBUtils
 * @Description TODO
 * @Date 2020/9/18 17:50
 * @Created by limeng
 */
object DBUtils {

  def getConection(): String = {
    new Random().nextInt(10) + ""
  }

  def returnConnection(connection:String): Unit ={

  }
}
