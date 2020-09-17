package com.lm.flink.dataset

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
 * @Classname DataSetDataSourceApp
 * @Description TODO
 * @Date 2020/9/17 20:48
 * @Created by limeng
 */
object DataSetDataSourceApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment


    val filePath = "D:\\workspace\\open_source\\flinkmoduletest\\src\\main\\resources\\file.txt"
    val line =  env.readTextFile(filePath)
    import org.apache.flink.api.scala._
    val value = line.flatMap(x=>{
      x.split(" ")
    })


    println("函数")
    line.flatMap(new MyFun).collect().foreach(println(_))
  }

  class MyFun extends FlatMapFunction[String, String]{
    override def flatMap(value: String, out: Collector[String]): Unit = {
      val s = value.split(" ")
      for (e <- s){
        out.collect(e)
      }
    }
  }


  def fromCollection(env: ExecutionEnvironment) ={
    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
  }


}
