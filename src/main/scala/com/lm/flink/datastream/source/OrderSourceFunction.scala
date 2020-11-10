package com.lm.flink.datastream.source

import java.util
import java.util.Collections

import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import lombok.extern.slf4j.Slf4j
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.types.Row
import java.lang.{Long => JavaLong}
import scala.util.Random
/**
 * @Classname OrderSourceFunction
 * @Description TODO
 * @Date 2020/11/9 20:07
 * @Created by limeng
 */

class OrderSourceFunction() extends SourceFunction[(Long, String,Int, Long)] with ListCheckpointed[JavaLong]{

  var sleepMs:Int = _
  var durationMs:Int = _
  var ms:Long = _

  val products = Array[String]("PC", "Gucci", "Channel", "YSL")

  def init()={
    ms = 0
    durationMs  =  10 * 1000
    sleepMs = (1000 / 10).toInt
  }


  override def run(ctx: SourceFunction.SourceContext[(Long, String, Int, Long)]): Unit = {

    while (ms < durationMs) {
      ctx.getCheckpointLock synchronized {
        for(i<- 0 until 100){
         // println("测试 i:"+i)
          ctx.collect((i + 0L,products(i % 4),new Random().nextInt(100),System.currentTimeMillis()))
        }
        ms += sleepMs
      }
      Thread.sleep(sleepMs)
    }

  }

  override def cancel(): Unit = {

  }


  override def snapshotState(checkpointId: Long, timestamp: Long): java.util.List[JavaLong] = Collections.singletonList(ms)

  override def restoreState(state: java.util.List[JavaLong]): Unit = {
    import scala.collection.JavaConverters._
    state.asScala.foreach(f=>{
      ms += f
    })
  }
}
object OrderSourceFunction{
  def apply(): OrderSourceFunction = {
    val function = new OrderSourceFunction()
    function.init()
    function
  }
}
