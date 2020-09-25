package com.lm.flink.datastream.example

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

import scala.collection.mutable
import scala.util.Random

/**
 * @Classname CustomParallelSourceFunction
 * @Description TODO
 * @Date 2020/9/21 15:19
 * @Created by limeng
 */
class CustomParallelSourceFunction extends ParallelSourceFunction[String] {

  var isRunning = true
  val random = new Random()
  val arrs=Array("You","know","some","birds","are","not","meant","to","be","caged")
  val size = arrs.size


  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while (isRunning){
      var index =  random.nextInt() % size
      if(index < 0){
        index = -index
      }
      val name = arrs(index)
      sourceContext.collect(name)
      Thread.sleep(10)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
