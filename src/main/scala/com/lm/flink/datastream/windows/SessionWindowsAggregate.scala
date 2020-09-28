package com.lm.flink.datastream.windows

import com.lm.flink.model.WordWithCount
import org.apache.flink.api.common.functions.{AggregateFunction, FlatMapFunction}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SessionWindowTimeGapExtractor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 会话窗口具有动态间隙的处理时间进行
 *
 *  在会话窗口中按活动会话分配器组中的数据元。与翻滚窗口和滑动窗口相比，会话窗口不重叠并且没有固定的开始和结束时间。
 *  相反，当会话窗口在一段时间内没有接收到数据元时，即当发生不活动的间隙时，会关闭会话窗口。会话窗口分配器可以配置
 *  静态会话间隙或 会话间隙提取器函数，该函数定义不活动时间段的长度。当此期限到期时，当前会话将关闭，后续数据元将分配给新的会话窗口。
 *
 * @Classname SessionWindowsAggregate
 * @Description TODO
 * @Date 2020/9/28 15:48
 * @Created by limeng
 */
object SessionWindowsAggregate {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val line = env.socketTextStream("localhost",9999)

    //设置事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    import org.apache.flink.api.scala._

//    line.flatMap(new FlatMapFunction[String,WordWithCount]{
//      override def flatMap(value: String, out: Collector[WordWithCount]): Unit = {
//        for(word <- value.split("\\s")){
//          out.collect(new WordWithCount(word,1L))
//        }
//      }
//    }).keyBy("word")
//      .windowAll(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[(String, Long)] {
//        override def extract(element: (String, Long)): Long = {
//          10000
//        }
//      })).trigger(new CustomProcessTimeTrigger())
//      .aggregate(new AverageAggregate())



    // 当前会话窗口结束才会输出值
//    aggregate
//      .print()
//      .setParallelism(1)

    env.execute("SessionWindowsAggregate")
  }
}
/**
 * 自定义触发器
 * 触发器决定了一个窗口何时可以被窗口函数处理，每一个窗口分配器都有一个默认的触发器
 *
 * CONTINUE： 没做什么，
 * FIRE：触发​​计算，
 * PURGE：清除窗口中的数据元和
 * FIRE_AND_PURGE：触发​​计算并清除窗口中的数据元。
 */
class CustomProcessTimeTrigger extends Trigger[(String,Long),TimeWindow]{
  var flag = 0

  /**
   * 每个元素被添加到窗口时使用
   * @param element
   * @param timestamp
   * @param window
   * @param ctx
   * @return
   */
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //注册定时器，当系统时间到达window end timestamp时会回调该trigger的onProcessingTime方法
    ctx.registerProcessingTimeTimer(window.maxTimestamp())
    // CONTINUE是代表不做输出，也就是，此时我们想要实现比如10条输出一次，
    //而不是窗口结束再输出就可以在这里实现。
    if(flag > 9){
      println("触发计算 -> flag: "+flag)
      flag = 0
      TriggerResult.FIRE
    }else{
      flag += 1
      println("onElement : " + element);
      TriggerResult.CONTINUE
    }
  }

  /**
   * 当一个已注册的处理时间计时器启动时调用
   *
   *  返回结果表示执行窗口计算并清空窗口
   * @param time
   * @param window
   * @param ctx
   * @return
   */
  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  /**
   *  当一个已注册的事件时间计时器启动时调用
   * @param time
   * @param window
   * @param ctx
   * @return
   */
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.FIRE_AND_PURGE
  }

  /**
   * 清除触发器可能仍为给定窗口保留的任何状态。
   * @param window
   * @param ctx
   */
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = ctx.deleteProcessingTimeTimer(window.maxTimestamp)

  override def toString: String = "ProcessingTimeTrigger()"

  /**
   * 如果此触发器支持合并触发器状态，则返回true
   *
   * @return
   */
  override def canMerge() = {
    true
  }

  /**
   * 与状态性触发器相关，当使用会话窗口时，两个触发器对应的窗口合并时，合并两个触发器的状态。
   *
   * @param window
   * @param ctx
   */
  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {

    val windowMaxTimestamp = window.maxTimestamp
    if (windowMaxTimestamp > ctx.getCurrentProcessingTime)
      ctx.registerProcessingTimeTimer(windowMaxTimestamp)
  }

}

class AverageAggregate extends AggregateFunction[(String,Long),(Long,Long),Double]{
  /**
   * 创建一个新的累加器，启动一个新的聚合
   * @return
   */
  override def createAccumulator(): (Long, Long) = {
    (0L,0L)
  }

  /**
   * 将给定的输入值添加到给定的累加器，返回new accumulator值
   * @param value
   * @param accumulator
   * @return
   */
  override def add(value: (String, Long), accumulator: (Long, Long)): (Long, Long) = {
    (accumulator._1 +value._2 , accumulator._2 + 1L)
  }

  /**
   * 从累加器获取聚合的结果
   * @param accumulator
   * @return
   */
  override def getResult(accumulator: (Long, Long)): Double = {
    println("触发: getResult 累加计算结果 \t" + accumulator._1 + "---->" + accumulator._2)
    accumulator._1
  }

  /**
   * 合并累加器获取聚合的结果
   * @param a
   * @param b
   * @return
   */
  override def merge(a: (Long, Long), b: (Long, Long)): (Long, Long) = {
    (a._1 + b._1, a._2 + b._2)
  }
}
