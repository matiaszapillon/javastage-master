package com.sandbox.javachallenge;

import org.springframework.boot.SpringApplication;

public class TestJavachallengeApplication {

    public static void main(String[] args) {
        SpringApplication.from(JavachallengeApplication::main).with(TestcontainersConfiguration.class).run(args);
    }

}
