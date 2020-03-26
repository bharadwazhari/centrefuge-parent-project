package com.kafka.stream.starter;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@ComponentScan(basePackages = "com.kafka.stream")
@Configuration
@EnableKafka
@EnableKafkaStreams
@EnableAutoConfiguration
public class KafkaStreamConfig {
}
