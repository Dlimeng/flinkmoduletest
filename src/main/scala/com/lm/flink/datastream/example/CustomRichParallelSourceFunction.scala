package com.lm.flink.datastream.example

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * @Classname CustomRichParallelSourceFunction
 * @Description TODO
 * @Date 2020/9/21 15:26
 * @Created by limeng
 */
class CustomRichParallelSourceFunction  extends RichParallelSourceFunction[Long] {
  var count = 1L
  var isRunning = true

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
