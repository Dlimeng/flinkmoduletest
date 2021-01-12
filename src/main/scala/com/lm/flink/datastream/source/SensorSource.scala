package com.lm.flink.datastream.source

import java.util.Calendar

import com.lm.flink.model.SensorReading
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}

import scala.util.Random

/**
 * @Classname SensorSource
 * @Description TODO
 * @Date 2021/1/12 12:10
 * @Created by limeng
 */
class SensorSource extends RichSourceFunction[SensorReading] {
  var running: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    val rand = new Random()

    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    var curFTemp = (1 to 10).map {
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
    }

    while (running) {

      // update temperature
      curFTemp = curFTemp.map( t => (t._1, t._2 + (rand.nextGaussian() * 0.5)) )
      // get current time
      val curTime = Calendar.getInstance.getTimeInMillis

      // emit new SensorReading
      curFTemp.foreach( t => ctx.collect(SensorReading(t._1, curTime, t._2)))

      // wait for 100 ms
      Thread.sleep(100)
    }

  }

  override def cancel(): Unit = {
    running = false
  }
}
