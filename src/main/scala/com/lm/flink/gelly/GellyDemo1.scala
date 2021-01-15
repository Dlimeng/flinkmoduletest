package com.lm.flink.gelly

import java.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.{Edge, Vertex, VertexJoinFunction}

import scala.collection.mutable.ListBuffer

/**
 * @Classname GellyDemo1
 * @Description TODO
 * @Date 2021/1/15 17:43
 * @Created by limeng
 */
object GellyDemo1 {
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
    val groupVD = graphs.joinWithVertices(roots,new VertexJoinFunction[Int,Int]{
      override def vertexJoin(vertexValue: Int, inputValue: Int): Int = {
        inputValue
      }
    }).mapVertices(m=>{
      if(m.getValue != Int.MaxValue){
        GroupVD(Set(MsgScore(m.getId, m.getId, m.getId, 100D)), Set[MsgScore](), Set(m.getId))
      }else{
        GroupVD(Set[MsgScore](), Set[MsgScore](), Set())
      }
    })





  }



  case class MsgScore(groupId: Long, from: Long, to: Long, score: Double) extends Serializable

  case class GroupVD(accept: Set[MsgScore], sent: Set[MsgScore], ids: Set[Long]) extends Serializable

  //memType: 1:集团成员 2:疑似集团成员
  case class GroupMem(targetId: Long, groupId: Long, score: Double,
                      memType: Int, tag: Int = 0, rel: Set[MemRel]) extends Serializable

  //typ 1:touzi 2:kongzhi
  case class MemRel(from: Long, to: Long, score: Double, typ: Int) extends Serializable{
    override def toString: String = from + "#" + to
  }



}
