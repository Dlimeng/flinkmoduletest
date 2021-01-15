package com.lm.flink.gelly;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageCombiner;
import org.apache.flink.graph.pregel.MessageIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * @Classname GellyDemo2
 * @Description TODO
 * @Date 2021/1/13 16:05
 * @Created by limeng
 * 迭代图计算
 * 最短距离
 * 让我们考虑使用以顶点为中心的迭代来计算单源最短路径。最初，每个顶点都具有无限距离的值，除了源顶点，其值为零。
 * 在第一个超级步骤期间，源将距离传播到其邻居。在以下超级步骤中，每个顶点检查其接收的消息并选择它们之间的最小距离。
 * 如果距离小于其当前值，则更新其状态并为其邻居生成消息。
 */
public class GellyDemo2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Vertex<Long, Double>> vs = new ArrayList<>();

//        vs.add(new Vertex<>(3L,Double.POSITIVE_INFINITY));
//        vs.add(new Vertex<>(2L,0d));
//        vs.add(new Vertex<>(5L,Double.POSITIVE_INFINITY));
//        vs.add(new Vertex<>(7L,Double.POSITIVE_INFINITY));
//
//        List<Edge<Long, Double>> es = new ArrayList<>();
//
//        es.add(new Edge<>(3L,7L,5d));
//        es.add(new Edge<>(5L,3L,3d));
//        es.add(new Edge<>(2L,5L,10d));
//        es.add(new Edge<>(5L,7L,2d));

        vs.add(new Vertex<>(1L,0d));
        vs.add(new Vertex<>(2L,Double.POSITIVE_INFINITY));
        vs.add(new Vertex<>(3L,Double.POSITIVE_INFINITY));
        vs.add(new Vertex<>(4L,Double.POSITIVE_INFINITY));
        vs.add(new Vertex<>(5L,Double.POSITIVE_INFINITY));
        vs.add(new Vertex<>(6L,Double.POSITIVE_INFINITY));
        vs.add(new Vertex<>(7L,Double.POSITIVE_INFINITY));

        List<Edge<Long, Double>> es = new ArrayList<>();

        es.add(new Edge<>(1L,2L,7d));
        es.add(new Edge<>(1L,4L,5d));
        es.add(new Edge<>(2L,3L,8d));
        es.add(new Edge<>(2L,4L,9d));
        es.add(new Edge<>(2L,5L,7d));
        es.add(new Edge<>(3L,5L,5d));
        es.add(new Edge<>(4L,5L,15d));
        es.add(new Edge<>(4L,6L,6d));
        es.add(new Edge<>(5L,6L,8d));
        es.add(new Edge<>(5L,7L,9d));
        es.add(new Edge<>(6L,7L,11d));

        Graph<Long, Double, Double> graphs = Graph.fromCollection(vs, es, env);

        int maxIterations = 10;

        Graph<Long, Double, Double> result  = graphs.runVertexCentricIteration(new SSSPComputeFunction(), new SSSPCombiner(), maxIterations);

        result.getVertices().print();

    }

    public static final class SSSPComputeFunction extends ComputeFunction<Long, Double,Double,Double> {
        @Override
        public void compute(Vertex<Long, Double> vertex, MessageIterator<Double> msgs) throws Exception {
            double minDistance = (vertex.getValue().equals(0d)) ? 0d:Double.POSITIVE_INFINITY;
            System.out.println("id :"+vertex.getId());
            for(Double msg:msgs){
                minDistance = Math.min(minDistance,msg);
            }
            System.out.println("minDistance:"+minDistance+" v: "+vertex.getValue());
            if(minDistance != Double.POSITIVE_INFINITY ){
                if(minDistance < vertex.getValue()){
                    this.setNewVertexValue(minDistance);
                }
                for(Edge<Long,Double> e:this.getEdges()){
                    System.out.println("t"+e.getTarget()+" v: "+e.getValue());
                    this.sendMessageTo(e.getTarget(),minDistance+e.getValue());
                }
            }
            System.out.println("-----------------------------");

        }
    }

    public static final class SSSPCombiner extends MessageCombiner<Long,Double> {
        @Override
        public void combineMessages(MessageIterator<Double> messages) throws Exception {
            double minMessage = Double.POSITIVE_INFINITY;
            for (Double msg:messages){
                minMessage = Math.min(minMessage,msg);
            }
            this.sendCombinedMessage(minMessage);
        }
    }


}
