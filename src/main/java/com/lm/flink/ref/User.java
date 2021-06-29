package com.lm.flink.ref;

import lombok.Data;

@Data
public class User implements UserInterface {
    private Integer id;
    private String name;

    @Override
    public void init() {
        System.out.println("init user");
    }
}
