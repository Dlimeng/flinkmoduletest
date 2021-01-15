package com.lm.flink.gelly.algorithm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.VertexJoinFunction;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Classname GellyDemo1
 * @Description TODO
 * @Date 2021/1/15 15:25
 * @Created by limeng
 */
public class GellyDemo1 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Vertex<Long, Integer>> vs = new ArrayList<>();

        vs.add(new Vertex<>(1L, 1));
        vs.add(new Vertex<>(2L, 1));
        vs.add(new Vertex<>(3L, 1));
        vs.add(new Vertex<>(4L, 1));
        vs.add(new Vertex<>(5L, 1));
        vs.add(new Vertex<>(6L, 1));
        vs.add(new Vertex<>(7L, 1));
        vs.add(new Vertex<>(8L, 1));

        List<Edge<Long, Double>> es = new ArrayList<>();

        es.add(new Edge<>(2L, 1L, 40d));
        es.add(new Edge<>(3L, 1L, 20d));
        es.add(new Edge<>(4L, 1L, 60d));
        es.add(new Edge<>(5L, 1L, 20d));
        es.add(new Edge<>(5L, 1L, 20d));
        es.add(new Edge<>(4L, 3L, 60d));
        es.add(new Edge<>(2L, 3L, 40d));
        es.add(new Edge<>(2L, 4L, 20d));
        es.add(new Edge<>(6L, 4L, 40d));
        es.add(new Edge<>(5L, 4L, 40d));
        es.add(new Edge<>(2L, 6L, 40d));
        es.add(new Edge<>(5L, 6L, 40d));
        es.add(new Edge<>(7L, 6L, 20d));
        es.add(new Edge<>(8L, 5L, 40d));

        Graph<Long, Integer, Double> graphs = Graph.fromCollection(vs, es, env);

        MapOperator<Tuple2<Long, LongValue>, Tuple2<Long, Integer>> roots = graphs.inDegrees().map(new MapFunction<Tuple2<Long, LongValue>, Tuple2<Long, Integer>>() {

            @Override
            public Tuple2<Long, Integer> map(Tuple2<Long, LongValue> value) throws Exception {
                if (value.f1.getValue() != 0) {
                    return Tuple2.of(value.f0, Integer.MAX_VALUE);
                } else {
                    return Tuple2.of(value.f0, value.f0.intValue());
                }
            }
        });


        graphs.joinWithVertices(roots, new VertexJoinFunction<Integer, Integer>() {
            @Override
            public Integer vertexJoin(Integer vertexValue, Integer inputValue) throws Exception {
                return inputValue;
            }
        }).mapVertices(new MapFunction<Vertex<Long, Integer>, CaseModel.GroupVD>() {
            @Override
            public CaseModel.GroupVD map(Vertex<Long, Integer> value) throws Exception {
                Set<CaseModel.MsgScore> accept = new HashSet<>();
                Set<CaseModel.MsgScore> sent = new HashSet<>();
                Set<Long> ids = new HashSet<>();
                CaseModel.GroupVD gg=null;
                if(value.getValue() != Integer.MAX_VALUE){
                    accept.add(new CaseModel.MsgScore(value.f0,value.f0,value.f0,100d));
                    ids.add(value.f0);
                    gg = new CaseModel.GroupVD(accept,sent,ids);
                }else{
                    gg =new CaseModel.GroupVD(accept,sent,ids);
                }
                return gg;
            }
        }).getVertices().print();
    }

}
