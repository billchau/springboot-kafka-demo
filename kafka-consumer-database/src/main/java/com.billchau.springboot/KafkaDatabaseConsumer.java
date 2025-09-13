package com.billchau.springboot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDatabaseConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatabaseConsumer.class);

    private WikimediaJPARepository wikimediaJPARepository;

    public KafkaDatabaseConsumer(WikimediaJPARepository wikimediaJPARepository) {
        this.wikimediaJPARepository = wikimediaJPARepository;
    }

    @KafkaListener(
            topics = "wikimedia_recentchange",
            groupId = "testgroup"
    )
    private void consume(String eventMsg) {
        LOGGER.info(String.format("message received -> %s", eventMsg));
        WikimediaData wikimediaData = new WikimediaData();

        wikimediaData.setWikiEventData(eventMsg);

        wikimediaJPARepository.save(wikimediaData);
    }

}
