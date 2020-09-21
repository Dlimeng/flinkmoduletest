package com.lm.flink.datastream.example

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
 * @Classname CustomParallelSourceFunction
 * @Description TODO
 * @Date 2020/9/21 15:19
 * @Created by limeng
 */
class CustomParallelSourceFunction extends ParallelSourceFunction[Long] {

  var isRunning = true
  var count = 1L

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning){
      sourceContext.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
