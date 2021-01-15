package com.lm.flink.gelly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.*;
import org.apache.flink.graph.asm.translate.translators.LongToLongValue;
import org.apache.flink.types.LongValue;

import java.util.ArrayList;
import java.util.List;

/**
 * @Classname Demo
 * @Description TODO
 * @Date 2021/1/7 18:38
 * @Created by limeng
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Vertex<Long, String>> vs = new ArrayList<>();

        vs.add(new Vertex<>(3L,"limeng"));
        vs.add(new Vertex<>(2L,"李四"));
        vs.add(new Vertex<>(5L,"张三"));
        vs.add(new Vertex<>(7L,"王五"));

        List<Edge<Long, String>> es = new ArrayList<>();

        es.add(new Edge<>(3L,7L,"测试关系"));
        es.add(new Edge<>(5L,3L,"测试指导"));
        es.add(new Edge<>(2L,5L,"测试同事"));
        es.add(new Edge<>(5L,7L,"测试朋友"));


        Graph<Long, String, String> graphs = Graph.fromCollection(vs, es, env);


        //图形转换操作 mapVertices mapEdges
        /**
         * (3,limeng1)
         * (2,李四1)
         * (5,张三1)
         * (7,王五1)
         * (3,7,测试关系)
         * (5,3,测试指导)
         * (2,5,测试同事)
         * (5,7,测试朋友)
         */
        Graph<Long, String, String> updatedGraph  = graphs.mapVertices(new MapFunction<Vertex<Long, String>, String>() {

            @Override
            public String map(Vertex<Long, String> value) throws Exception {
                return value.getValue()+"1";
            }
        });

        /**
         * (3,limeng)
         * (2,李四)
         * (5,张三)
         * (7,王五)
         * (3,7,测试关系1)
         * (5,3,测试指导1)
         * (2,5,测试同事1)
         * (5,7,测试朋友1)
         */
        Graph<Long, String, String> updatedGraph2 = graphs.mapEdges(new MapFunction<Edge<Long, String>, String>() {
            @Override
            public String map(Edge<Long, String> value) throws Exception {
                return value.getValue()+"1";
            }
        });


        /**
         *translateGraphIDs translateVertexValues translateEdgeValues  id vertex edge 转换类型
         */

        /**
         * filterOnVertices  过滤结点
         * filterOnEdges 过滤边
         * (3,3)
         * (2,2)
         * (5,5)
         * (7,7)
         * (5,3,测试指导)
         * (3,7,测试关系)
         * (5,7,测试朋友)
         * (2,5,测试同事)
         */
        Graph<Long, Long, String> updatedGraph3 = graphs.mapVertices(new MapFunction<Vertex<Long, String>, Long>() {

            @Override
            public Long map(Vertex<Long, String> value) throws Exception {
                return value.getId();
            }
        }).filterOnVertices(f -> f.getValue() > 0);

//        updatedGraph3.getVertices().print();
//        updatedGraph3.getEdges().print();

        /**
         * 划分子图
         * graph.subgraph(
         *         new FilterFunction<Vertex<Long, Long>>() {
         *                    public boolean filter(Vertex<Long, Long> vertex) {
         *                     // keep only vertices with positive values
         *                     return (vertex.getValue() > 0);
         *                }
         *            },
         *         new FilterFunction<Edge<Long, Long>>() {
         *                 public boolean filter(Edge<Long, Long> edge) {
         *                     // keep only edges with negative values
         *                     return (edge.getValue() < 0);
         *                 }
         *         })
         */
        Graph<Long, Long, String> updatedGraph4 = updatedGraph3.subgraph((v -> v.getValue() > 0), (e -> e.getValue() != null));

        DataSet<Tuple2<Long, LongValue>> ins = updatedGraph3.inDegrees();

        //System.out.println("ins");
        //ins.print();
        /**
         * Join joinWithVertices  joinWithEdges  joinWithEdgesOnSource joinWithEdgesOnTarget
         */
        Graph<Long, Long, String> updatedGraph5 = updatedGraph3.joinWithVertices(ins, new VertexJoinFunction<Long, LongValue>() {
            @Override
            public Long vertexJoin(Long vertexValue, LongValue inputValue) throws Exception {
                return vertexValue + inputValue.getValue();
            }
        });
        System.out.println("updatedGraph5");
        updatedGraph5.getVertices().print();
        updatedGraph5.getEdges().print();

        //所有边的方向都已经反转
        /**
         * (3,3)
         * (2,2)
         * (5,5)
         * (7,7)
         * (3,5,测试指导)
         * (7,5,测试朋友)
         * (7,3,测试关系)
         * (5,2,测试同事)
         */
        Graph<Long, Long, String> updatedGraphReverse = updatedGraph3.reverse();

//        updatedGraphReverse.getVertices().print();
//        updatedGraphReverse.getEdges().print();

        /**
         * 双向边 都展示出来
         * (3,3)
         * (2,2)
         * (5,5)
         * (7,7)
         * (5,3,测试指导)
         * (3,5,测试指导)
         * (3,7,测试关系)
         * (7,3,测试关系)
         * (5,7,测试朋友)
         * (7,5,测试朋友)
         * (2,5,测试同事)
         * (5,2,测试同事)
         */
        Graph<Long, Long, String> updatedGraphUndirected = updatedGraph3.getUndirected();

//        updatedGraphUndirected.getVertices().print();
//        updatedGraphUndirected.getEdges().print();


        /**
         * union 并集
         * difference 差集
         * Intersect 交集
         */

        /**
         * Gelly包含以下用于在输入中添加和删除顶点和边的方法
         *
         */
        Graph<Long, Long, String> updatedGraphAddVertex = updatedGraph3.addVertex(new Vertex<Long, Long>(1L, 1L));

//        updatedGraphAddVertex.getVertices().print();
//        updatedGraphAddVertex.getEdges().print();


        Graph<Long, Long, Double> updatedGraph6 = updatedGraph3.mapEdges(new MapFunction<Edge<Long, String>, Double>() {
            @Override
            public Double map(Edge<Long, String> value) throws Exception {
                long v = value.f0 + value.f1;
                return Double.valueOf(v);
            }
        });
//        System.out.println("updatedGraph6");
//        updatedGraph6.getVertices().print();
//        updatedGraph6.getEdges().print();
        /**
         * reduceOnEdges()可以用于计算顶点的相邻边缘的值的聚合，并且reduceOnNeighbors()可以用于计算相邻顶点的值的聚合。
         * (3,10.0)
         * (5,8.0)
         * (2,7.0)
         */
        DataSet<Tuple2<Long, Double>> updatedGraphReduceOnEdges = updatedGraph6.reduceOnEdges(new SelectMinWeight(), EdgeDirection.OUT);

//        System.out.println("updatedGraphReduceOnEdges");
//        updatedGraphReduceOnEdges.print();


        /**
         * groupReduceOnEdges()
         * groupReduceOnNeighbors()
         */

        /**
         * 图验证 它检查边集是否包含有效的顶点ID，即所有边ID都存在于顶点ID集中。
         * graph.validate(new InvalidVertexIdsValidator<Long, Long, Long>());
         */

    }



    final static class SelectMinWeight implements ReduceEdgesFunction<Double>{
        @Override
        public Double reduceEdges(Double firstEdgeValue, Double secondEdgeValue) {
            return Math.min(firstEdgeValue,secondEdgeValue);
        }
    }
}
