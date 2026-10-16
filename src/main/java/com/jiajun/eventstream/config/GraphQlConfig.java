package com.jiajun.eventstream.config;

import com.jiajun.eventstream.graphql.scalar.DateTimeScalar;
import com.jiajun.eventstream.graphql.scalar.JsonScalar;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.execution.RuntimeWiringConfigurer;

@Configuration
public class GraphQlConfig {

    @Bean
    public RuntimeWiringConfigurer runtimeWiringConfigurer() {
        return builder -> builder
                .scalar(JsonScalar.INSTANCE)
                .scalar(DateTimeScalar.INSTANCE);
    }
}
