package com.lm.flink.gelly;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * @Classname GellyDemo3
 * @Description TODO
 * @Date 2021/1/14 15:49
 * @Created by limeng
 *配置以顶点为中心的迭代
 *可以使用该registerAggregator()方法注册迭代聚合器。迭代聚合器按超级步骤全局组合所有聚合，并使它们在下一个超级步骤中可用。
 * 可以在用户定义的内部访问已注册的聚合器ComputeFunction。
 * 广播变量
 *
 *
 */
public class GellyDemo3 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Vertex<Long, Long>> vs = new ArrayList<>();

        vs.add(new Vertex<>(3L,3L));
        vs.add(new Vertex<>(2L,2L));
        vs.add(new Vertex<>(5L,5L));
        vs.add(new Vertex<>(7L,7L));

        List<Edge<Long, Double>> es = new ArrayList<>();

        es.add(new Edge<>(3L,7L,5d));
        es.add(new Edge<>(5L,3L,3d));
        es.add(new Edge<>(2L,5L,10d));
        es.add(new Edge<>(5L,7L,2d));


        Graph<Long, Long, Double> graphs = Graph.fromCollection(vs, es, env);

        int maxIterations = 10;

        VertexCentricConfiguration parameters  = new VertexCentricConfiguration();

        parameters.setName("Gelly Iteration");
        parameters.setParallelism(10);


        parameters.registerAggregator("sumAggregator",new LongSumAggregator());

        Graph<Long, Long, Double> res = graphs.runVertexCentricIteration(
                new Compute(), null, maxIterations, parameters);

        res.getVertices().print();
        res.getEdges().print();


    }

    public static final class Compute extends ComputeFunction<Long,Long,Double,Long>{
        LongSumAggregator aggregator = new LongSumAggregator();

        @Override
        public void preSuperstep() throws Exception {
            aggregator = this.getIterationAggregator("sumAggregator");
        }

        @Override
        public void compute(Vertex<Long, Long> vertex, MessageIterator<Long> longs) throws Exception {


            aggregator.aggregate(vertex.getValue());



        }
    }
}
