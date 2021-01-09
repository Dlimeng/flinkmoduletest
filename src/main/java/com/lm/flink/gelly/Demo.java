package com.lm.flink.gelly;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

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




    }
}
