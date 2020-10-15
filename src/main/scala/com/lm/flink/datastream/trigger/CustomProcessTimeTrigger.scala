package com.lm.flink.datastream.trigger

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @Classname CustomProcessTimeTrigger
 * @Description TODO
 * @Date 2020/10/15 21:01
 * @Created by limeng
 *
 *
 */
class CustomProcessTimeTrigger extends Trigger[Int,TimeWindow]{
  override def onElement(element: Int, timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = ???

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = ???

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = ???

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = ???
}
