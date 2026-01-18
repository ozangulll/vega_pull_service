package com.vega.pullservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

@SpringBootApplication
@EnableJpaAuditing
public class VegaPullServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(VegaPullServiceApplication.class, args);
    }
}




