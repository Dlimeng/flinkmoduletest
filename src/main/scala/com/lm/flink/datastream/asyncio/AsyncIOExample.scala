package com.lm.flink.datastream.asyncio

import java.beans.Transient
import java.util
import java.util.concurrent.{ExecutorService, Executors, ThreadLocalRandom, TimeUnit}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{AsyncDataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.ExecutorUtils

/**
 * @Classname AsyncIOExample
 * @Description TODO
 * @Date 2021/1/21 18:59
 * @Created by limeng
 */
object AsyncIOExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(10)
    import org.apache.flink.api.scala._
    val  inputStream = env.addSource(new CustomNonParallelSourceFunction)

    val result1 = AsyncDataStream.orderedWait(inputStream,new SampleAsyncFunction,1000,TimeUnit.MILLISECONDS,20)
    val result2 = AsyncDataStream.unorderedWait(inputStream,new SampleAsyncFunction,1000,TimeUnit.MILLISECONDS,20)


    result1.print("result1")
    result2.print("result2")

    env.execute("AsyncIOExample")

  }

  class CustomNonParallelSourceFunction extends SourceFunction[Long] {
    var count = 0L
    var isRunning = true

    override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
      while (isRunning){
        sourceContext.collect(count)
        count +=1
        Thread.sleep(1000)
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }
  val executorService:ExecutorService = Executors.newFixedThreadPool(30)
  class SampleAsyncFunction extends RichAsyncFunction[Long,String] {

    val failRatio = 0.001f
    val sleepFactor = 1000L
    val shutdownWaitTS = 20000L

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
    }

    override def close(): Unit = {
      super.close()
      ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MILLISECONDS, executorService)
    }

    override def asyncInvoke(input: Long, resultFuture: ResultFuture[String]): Unit = {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val sleep = (ThreadLocalRandom.current().nextFloat() * sleepFactor).toLong
          try {
            Thread.sleep(sleep)

            if(ThreadLocalRandom.current().nextFloat() < failRatio){
              resultFuture.completeExceptionally(new Exception("lilili"))
            }else resultFuture.complete(List("key-" + input))

          }catch {
            case e:Exception=>{
              resultFuture.complete(List())
              e.printStackTrace()
            }
          }
        }
      })
    }
  }
}
