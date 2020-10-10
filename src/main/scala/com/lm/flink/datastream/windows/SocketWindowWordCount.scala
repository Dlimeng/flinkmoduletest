package com.lm.flink.datastream.windows

import com.lm.flink.model.WordWithCount
import org.apache.flink.api.common.functions.{FlatMapFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @Classname SocketWindowWordCount
 * @Description TODO
 * @Date 2020/9/28 15:08
 * @Created by limeng
 */
object SocketWindowWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val line = env.socketTextStream("localhost",9999)

    import org.apache.flink.api.scala._

    val windowCounts = line.flatMap(new FlatMapFunction[String,WordWithCount] {
      override def flatMap(value: String, out: Collector[WordWithCount]): Unit = {
        for(word <- value.split("\\s")){
          out.collect(new WordWithCount(word,1L))
        }
      }
    }).keyBy("word")
      .timeWindow(Time.seconds(5),Time.seconds(10))
      .reduce(new ReduceFunction[WordWithCount] {
        override def reduce(a: WordWithCount, b: WordWithCount): WordWithCount = {
          return new WordWithCount(a.word,a.count+b.count)
        }
      })


    windowCounts.print().setParallelism(1)

    env.execute("SocketWindowWordCount")

  }
}
