package com.lm.flink.datastream.source

import java.util
import java.util.Collections

import org.apache.flink.streaming.api.checkpoint.{CheckpointedFunction, ListCheckpointed}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import lombok.extern.slf4j.Slf4j
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.types.Row

import scala.util.Random
/**
 * @Classname OrderSourceFunction
 * @Description TODO
 * @Date 2020/11/9 20:07
 * @Created by limeng
 */

class OrderSourceFunction(val numKeys:Int,val rowsPerKeyAndSecond:Float,val durationSeconds:Int,val offsetSeconds:Int) extends SourceFunction[(Long, String,Int, Long),ListCheckpointed[Long]]{

  var sleepMs:Int = _
  var durationMs:Int = _
  var ms:Long = _

  val products = Array[String]("PC", "Gucci", "Channel", "YSL")


  def apply(numKeys: Int, rowsPerKeyAndSecond: Float, durationSeconds: Int, offsetSeconds: Int): OrderSourceFunction = {
    durationMs  =  durationSeconds * 1000
    sleepMs = (1000 / rowsPerKeyAndSecond).toInt
    new OrderSourceFunction(numKeys, rowsPerKeyAndSecond, durationSeconds, offsetSeconds)
  }


  override def run(ctx: SourceFunction.SourceContext[(Long, String, Int, Long)]): Unit = {
    val offsetMS: Long = offsetSeconds * 2000L
    import scala.collection.JavaConverters._

    while (ms < durationMs) {
      ctx.getCheckpointLock synchronized {
        for(i<- 0 until(numKeys)){
          ctx.collect((i + 0L,products(i % 4),new Random().nextInt(100),System.currentTimeMillis()))
        }
      }
      Thread.sleep(sleepMs)
    }

  }

  override def cancel(): Unit = {

  }



  override def snapshotState(checkpointId: Long, timestamp: Long): java.util.List[Long] = Collections.singletonList(ms)

  override def restoreState(state: java.util.List[Long]): Unit = {
    import scala.collection.JavaConverters._
    state.asScala.foreach(f=>{
      ms += f
    })
  }
}
