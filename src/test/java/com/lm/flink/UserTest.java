package com.lm.flink;

import com.lm.flink.ref.UserInterface;
import org.reflections.Reflections;

import java.util.Set;

public class UserTest {
    public static void main(String[] args) {
        Reflections reflections = new Reflections("com.lm.flink.ref", UserInterface.class);
        Set<Class<? extends UserInterface>> subTypesOf = reflections.getSubTypesOf(UserInterface.class);
        subTypesOf.stream().forEach(f->{
            System.out.println(f.getName());
        });
    }
}
