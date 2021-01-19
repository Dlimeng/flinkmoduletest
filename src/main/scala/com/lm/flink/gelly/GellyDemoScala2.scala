package com.lm.flink.gelly

import java.util

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.graph.{Edge, Vertex, VertexJoinFunction}
import org.apache.flink.graph.scala.Graph
import org.apache.flink.graph.spargel.{GatherFunction, MessageIterator, ScatterFunction}

import scala.collection.mutable.ListBuffer

/**
 * @Classname GellyDemo2
 * @Description TODO
 * @Date 2021/1/19 16:08
 * @Created by limeng
 */
object GellyDemoScala2 {
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


    val inAndOuts = graphs.joinWithVertices(roots,new VertexJoinFunction[Int,Int]{
      override def vertexJoin(vertexValue: Int, inputValue: Int): Int = {
        inputValue
      }
    }).mapVertices(v=>{
      if (v.getValue != Int.MaxValue) {
        InAndOut(List(FromInfo(v.getId, true,null)), List[FromInfo]())
      }else{
        InAndOut(List(FromInfo(v.getId, false,null)), List[FromInfo]())
      }
    })


    val subGraphs =inAndOuts.runScatterGatherIteration(new SendMsgIn,new MergeMsgIn,10).mapVertices(m=>{
      m.getValue.in.filter(_.srcId != m.getId)
    })

    subGraphs.getVertices.filter(_.getValue.nonEmpty).map(m=>(m.getId,m.getValue.filter(_.root).distinct.sortBy(_.path.split("#").length))).print()

  }

  class SendMsgIn extends  ScatterFunction[Long,InAndOut,List[MsgFlag],Double]{
    override def sendMessages(vertex: Vertex[Long, InAndOut]): Unit = {
      var tm = vertex.getValue.in.diff(vertex.getValue.out)
      if(tm.nonEmpty){
        val iter = this.getEdges.iterator()


        while (iter.hasNext){
          val e = iter.next()

            val in = tm.map(r=>{
              val p = r.path
              if(p == null){
                MsgFlag(r.srcId,0,r.root,r.srcId.toString)
              }else{
                MsgFlag(r.srcId,0,r.root,p+"#"+e.getSource)
              }
            })

            val out = tm.map(r => MsgFlag(r.srcId, 1, r.root,r.path))

            this.sendMessageTo(e.getTarget,in)
            this.sendMessageTo(e.getSource,out)

        }
      }

    }
  }

  class MergeMsgIn extends GatherFunction[Long,InAndOut,List[MsgFlag]]{
    override def updateVertex(vertex: Vertex[Long,InAndOut], inMessages: MessageIterator[List[MsgFlag]]): Unit ={

      import collection.JavaConverters._

      val list = new util.ArrayList[MsgFlag]()
      while (inMessages.hasNext){
        val in = inMessages.next().asJava
        list.addAll(in)
      }

      if(list.isEmpty) this.setNewVertexValue(vertex.getValue)
      else{
        val v = vertex.getValue
        val news = list.asScala.toList

        val in = v.in ++ news.filter(_.flag == 0).map(r=> FromInfo(r.srcId,r.root,r.path))
        if(v.out == null){
          val out = news.filter(_.flag == 1).map(r=> FromInfo(r.srcId,r.root,r.path))
          this.setNewVertexValue(InAndOut(in,out))
        }else{
          val out = v.out ++ news.filter(_.flag == 1).map(r => FromInfo(r.srcId,  r.root,r.path))
          this.setNewVertexValue(InAndOut(in,out))
        }
      }
    }
  }



  case class InAndOut(in:List[FromInfo],out:List[FromInfo]) extends Serializable

  // flag 1 给OUT  0 给In
  case class MsgFlag(srcId: Long,  flag: Int, root: Boolean,path:String) extends Serializable {
    override def toString: String = srcId + " # " + flag
  }

  case class FromInfo(srcId:Long, root: Boolean,path:String) extends Serializable{
    override def hashCode(): Int = srcId.hashCode()

    override def equals(obj: Any): Boolean = {
      if(obj == null) false else{
        val o = obj.asInstanceOf[FromInfo]
        o.srcId.equals(this.srcId)
      }
    }

    override def toString: String = srcId+"#"+ root.toString+"#"+path
  }


}
