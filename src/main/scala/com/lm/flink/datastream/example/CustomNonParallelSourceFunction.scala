package com.lm.flink.datastream.example

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * @Classname CustomNonParallelSourceFunction
 * @Description TODO
 * @Date 2020/9/21 15:01
 * @Created by limeng
 *  非并行
 */
class CustomNonParallelSourceFunction extends SourceFunction[Long] {
  var count = 1L
  var isRunning = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    if(isRunning){
      sourceContext.collect(count)
      count +=1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
