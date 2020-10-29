package com.lm.flink.datastream.join

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Classname TumblingWindowJoin
 * @Description TODO
 * @Date 2020/10/27 18:38
 * @Created by limeng
 */
object TumblingWindowJoin {
  def main(args: Array[String]): Unit = {
    //构建运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置最少一次和恰一次处理语义
    env.enableCheckpointing(20000,CheckpointingMode.EXACTLY_ONCE)

    //设置checkpoint 目录
    env.getCheckpointConfig
      .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    




  }
}
