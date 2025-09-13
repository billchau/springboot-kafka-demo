package com.billchau.springboot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Service
public class WikimediaChangesProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesProducer.class);

    private KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Flux<String> consumeWikimediaStream() {
        WebClient client = WebClient.create();

        return client.get()
                .uri("https://stream.wikimedia.org/v2/stream/recentchange")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .bodyToFlux(String.class)
                .timeout(Duration.ofSeconds(5))
                .doOnNext(event -> {
                    // Process each event (JSON string)
                    System.out.println("Received: " + event);
                    kafkaTemplate.send("wikimedia_recentchange", event);
                })
                .doOnError(error -> System.err.println("Error: " + error.getMessage()));
    }


}
