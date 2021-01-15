package com.lm.flink.gelly;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Classname GellyDemo4
 * @Description TODO
 * @Date 2021/1/14 16:23
 * @Created by limeng
 * 分散 - 聚集模型，也称为“信号/收集”模型，从图中的顶点的角度表示计算。
 * 分散 ，生成顶点将发送到其他顶点的消息
 * 收集，使用收到的消息更新顶点值，Gelly 提供了分散-聚集迭代的方法，用户只需要实现两个函数，对应于分散和聚集阶段。
 * 第一个函数ScatterFunction，允许顶点将消息发送到其他顶点，在发送过程中接收消息。
 * 第二个函数GatherFunction，它定义了顶点如何根据接收的消息更新其值，这些函数和运行的最大迭代次数作为Gelly的参数给出runScatterGatherIteration。此方法将对输入Graph执行分散 - 聚集迭代，并返回具有更新顶点值的新Graph。
 */
public class GellyDemo4 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Vertex<Long, Double>> vs = new ArrayList<>();

        vs.add(new Vertex<>(3L,Double.POSITIVE_INFINITY));
        vs.add(new Vertex<>(2L,0d));
        vs.add(new Vertex<>(5L,Double.POSITIVE_INFINITY));
        vs.add(new Vertex<>(7L,Double.POSITIVE_INFINITY));

        List<Edge<Long, Double>> es = new ArrayList<>();

        es.add(new Edge<>(3L,7L,5d));
        es.add(new Edge<>(5L,3L,3d));
        es.add(new Edge<>(2L,5L,10d));
        es.add(new Edge<>(5L,7L,2d));


        Graph<Long, Double, Double> graphs = Graph.fromCollection(vs, es, env);

        int maxIterations = 10;

        Graph<Long, Double, Double> res = graphs.runScatterGatherIteration(new MinDistanceMessenger(), new VertexDistanceUpdater(), maxIterations);

        res.getVertices().print();
        res.getEdges().print();

    }

    public static final class MinDistanceMessenger extends ScatterFunction<Long, Double, Double, Double>{

        @Override
        public void sendMessages(Vertex<Long, Double> vertex) throws Exception {
            for (Edge<Long,Double> edge:this.getEdges()){
                this.sendMessageTo(edge.getTarget(),vertex.getValue() + edge.getValue());
            }
        }
    }

    public static final class VertexDistanceUpdater extends GatherFunction<Long,Double,Double> {

        @Override
        public void updateVertex(Vertex<Long, Double> vertex, MessageIterator<Double> inMessages) throws Exception {
            Double minDistance = Double.MAX_VALUE;

            for (double msg:inMessages){
                if(msg < minDistance){
                    minDistance = msg;
                }
            }

            if(vertex.getValue() > minDistance){
                this.setNewVertexValue(minDistance);
            }

        }
    }

}
