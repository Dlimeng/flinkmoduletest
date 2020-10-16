package com.lm.flink.datastream.state

import org.apache.commons.compress.utils.Lists
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import collection.JavaConversions._
/**
 * @Classname KeyStateSteamTest
 * @Description TODO
 * @Date 2020/10/16 19:17
 * @Created by limeng
 */
object KeyStateSteamTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.fromElements(Tuple2("a", 50L), Tuple2("a", 80L), Tuple2("a", 400L),
      Tuple2("a", 100L), Tuple2("a", 200L), Tuple2("a", 200L),
      Tuple2("b", 100L), Tuple2("b", 200L), Tuple2("b", 200L),
      Tuple2("b", 500L), Tuple2("b", 600L), Tuple2("b", 700L))
      .keyBy(0)
      .flatMap(new ThresholdWarning(100L,3))  //超过100阈值3次后报警
      .print()

    env.execute("KeyStateSteamTest")
  }
}

class  ThresholdWarning(val threshold:Long,val numberOfTimes:Int) extends RichFlatMapFunction[Tuple2[String,Long],Tuple2[String,List[Long]]]{

  @transient
  var abnormalData:ListState[Long] =_

  override def open(parameters: Configuration): Unit = {
    abnormalData = getRuntimeContext.getListState(new ListStateDescriptor("abnormalData",createTypeInformation[Long]))
  }
  override def flatMap(value: (String, Long), out: Collector[(String, List[Long])]): Unit = {
    val inputValue:Long = value._2
    //如果输入值超过阈值，则记录该次不正常数据信息
    if(inputValue >= threshold){
      abnormalData.add(inputValue)
    }

    val list = Lists.newArrayList(abnormalData.get().iterator())

    //如果不正常的数据出现达到一定次数，则输出报警信息
    if(list.size() >= numberOfTimes){
      out.collect(Tuple2(value._1 + " 超过阈值 ",list.toList))
      //报警信息输出后，清空状态
      abnormalData.clear()
    }
  }
}