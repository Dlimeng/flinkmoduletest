package com.lm.flink.gelly;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;

import org.apache.flink.graph.spargel.MessageIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * @Classname GellyDemo5
 * @Description TODO
 * @Date 2021/1/14 16:48
 * @Created by limeng
 * Gather-Sum-Apply
 * 收集，在每个顶点的边和邻居上并行调用用户定义的函数，从而生成部分值。
 * 总和，使用用户定义的reduce将聚集阶段中生成的部分值聚合单个值。
 * 应用，通过对当前值和Sum阶段生成的聚合值应用函数来更新每个顶点值，
 * 让我们考虑在下图中用GSA计算单源最短路径，并让顶点1成为源。在该Gather阶段期间，我们通过将每个顶点值与边缘权重相加来计算新的候选距离。在Sum，候选距离按顶点ID分组，并选择最小距离。
 * 在Apply，将新计算的距离与当前顶点值进行比较，并将两者中的最小值指定为顶点的新值。
 */
public class GellyDemo5 {
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
        Graph<Long, Double, Double> res = graphs.runGatherSumApplyIteration(new CalculateDistances(), new ChooseMinDistance(), new UpdateDistance(), maxIterations);

        res.getVertices().print();
        res.getEdges().print();
    }

    //Gather
    public static final class CalculateDistances extends GatherFunction<Double,Double,Double> {

        @Override
        public Double gather(Neighbor<Double, Double> neighbor) {
            return neighbor.getNeighborValue() + neighbor.getEdgeValue();
        }
    }

    //sum
    public static final class ChooseMinDistance extends SumFunction<Double,Double,Double>{

        @Override
        public Double sum(Double newValue, Double currentValue) {
            return Math.min(newValue,currentValue);
        }
    }

    public static final class UpdateDistance extends ApplyFunction<Long,Double,Double> {

        @Override
        public void apply(Double newDistance, Double oldDistance) {
            if(newDistance < oldDistance){
                this.setResult(newDistance);
            }
        }
    }
}
