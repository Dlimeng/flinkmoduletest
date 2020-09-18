package com.lm.flink.dataset

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode


/**
 * @Classname DataSetSinkApp
 * @Description TODO
 * @Date 2020/9/18 15:52
 * @Created by limeng
 */
object DataSetSinkApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val data = 1 to 10
    val text = env.fromCollection(data)

    val filePath = "fileResult.txt"
    text.writeAsText(filePath,WriteMode.OVERWRITE)

    env.execute("SinkApp")
  }
}
