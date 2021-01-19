package com.lm.flink.gelly

import java.util

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.graph.pregel.ComputeFunction
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.spargel.{GatherFunction, MessageIterator, ScatterFunction}
import org.apache.flink.graph.{Edge, Vertex, VertexJoinFunction}

import scala.collection.mutable.ListBuffer

/**
 * @Classname GellyDemo1
 * @Description TODO
 * @Date 2021/1/15 17:43
 * @Created by limeng
 */
object GellyDemoScala1 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val vs = new ListBuffer[Vertex[Long,Int]]

    vs.append(new Vertex(1L, 0))
    vs.append(new Vertex(2L, 0))
    vs.append(new Vertex(3L, 0))
    vs.append(new Vertex(4L, 0))
    vs.append(new Vertex(5L, 0))
    vs.append(new Vertex(6L, 0))
    vs.append(new Vertex(7L, 0))
    vs.append(new Vertex(8L, 0))


    val es = new ListBuffer[Edge[Long, Double]]

    es.append(new Edge[Long, Double](2L, 1L, 40d))
    es.append(new Edge[Long, Double](3L, 1L, 20d))
    es.append(new Edge[Long, Double](4L, 1L, 60d))
    es.append(new Edge[Long, Double](5L, 1L, 20d))
    es.append(new Edge[Long, Double](5L, 1L, 20d))
    es.append(new Edge[Long, Double](4L, 3L, 60d))
    es.append(new Edge[Long, Double](2L, 3L, 40d))
    es.append(new Edge[Long, Double](2L, 4L, 20d))
    es.append(new Edge[Long, Double](6L, 4L, 40d))
    es.append(new Edge[Long, Double](5L, 4L, 40d))
    es.append(new Edge[Long, Double](2L, 6L, 40d))
    es.append(new Edge[Long, Double](5L, 6L, 40d))
    es.append(new Edge[Long, Double](7L, 6L, 20d))
    es.append(new Edge[Long, Double](8L, 5L, 40d))


    val graphs : Graph[Long, Int, Double] = Graph.fromCollection(vs,es,env)

    //入度为0
    val roots = graphs.inDegrees().map(f=> {
      if(f._2.getValue != 0){
        (f._1,Int.MaxValue)
      }else{
        (f._1,f._1.intValue())
      }
    })

    //groupvd
    val groupVD: Graph[Long, GroupVD, Double] = graphs.joinWithVertices(roots, new VertexJoinFunction[Int, Int] {
      override def vertexJoin(vertexValue: Int, inputValue: Int): Int = {
        inputValue
      }
    }).mapVertices(m => {
      if (m.getValue != Int.MaxValue) {
        GroupVD(Set(MsgScore(m.getId, m.getId, m.getId, 100D)), Set[MsgScore](), Set(m.getId))
      } else {
        GroupVD(Set[MsgScore](), Set[MsgScore](), Set())
      }
    })

    val quaGroup = groupVD.runScatterGatherIteration(new GSendMsg,new GMergeMsg,10)

    println("quaGroup.getVertices")
    quaGroup.getVertices.print()

    val mm = quaGroup.getVertices.flatMap(f=>{
      f.getValue.accept.groupBy(_.groupId).map(m=>{
        val rel = m._2.map(msg=>{
          MemRel(msg.from,msg.to,msg.score,1)
        })
        GroupMem(f.getId, m._1, m._2.map(_.score).sum, 1, 0, rel)
      })
    })


    mm.sortPartition(_.targetId,Order.ASCENDING).print()







  }

  class GSendMsg extends ScatterFunction[Long,GroupVD,GroupVD,Double]{
    override def sendMessages(vertex: Vertex[Long, GroupVD]): Unit = {
      val iter = this.getEdges.iterator()

      while (iter.hasNext){
        val edge = iter.next()
        val from = edge.getSource
        val to = edge.getTarget

        val gvd = vertex.getValue

        val unsent = gvd.accept.diff(gvd.sent)

        if(unsent.nonEmpty && !gvd.ids.contains(to)){

          val msg = unsent.map(a=>{
            MsgScore(a.groupId,from,to,edge.getValue)
          })

          this.sendMessageTo(to, GroupVD(msg, Set[MsgScore](), gvd.ids ++ Set(to)))
          this.sendMessageTo(from, GroupVD(Set[MsgScore](), msg, gvd.ids ))
        }

      }
    }
  }

  class GMergeMsg extends GatherFunction[Long,GroupVD,GroupVD]{
    override def updateVertex(vertex: Vertex[Long, GroupVD], inMessages: MessageIterator[GroupVD]): Unit = {
      val iter = inMessages.iterator()
      var vv = vertex.getValue


      while (iter.hasNext){
        val edge = iter.next()
        vv = GroupVD(vv.accept ++ edge.accept,vv.sent ++ edge.sent,vv.ids ++ edge.ids)
      }

      this.setNewVertexValue(vv)
    }
  }



  case class MsgScore(groupId: Long, from: Long, to: Long, score: Double) extends Serializable{
    override def toString: String = from + "#" + to + "#"+score
  }

  case class GroupVD(accept: Set[MsgScore], sent: Set[MsgScore], ids: Set[Long]) extends Serializable

  //memType: 1:集团成员 2:疑似集团成员
  case class GroupMem(targetId: Long, groupId: Long, score: Double,
                      memType: Int, tag: Int = 0, rel: Set[MemRel]) extends Serializable

  //typ 1:touzi 2:kongzhi
  case class MemRel(from: Long, to: Long, score: Double, typ: Int) extends Serializable{
    override def toString: String = from + "#" + to
  }



}
