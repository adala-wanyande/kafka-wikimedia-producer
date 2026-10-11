package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WikiMediaChangesProducer {

    private static final Logger log =
            LoggerFactory.getLogger(WikiMediaChangesProducer.class);

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "wikimedia.recentchange";
        String url =
                "https://stream.wikimedia.org/v2/stream/recentchange";

        // Create producer properties
        Properties properties = new Properties();

        properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );
        properties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );
        properties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        KafkaProducer<String, String> producer =
                new KafkaProducer<>(properties);

        AtomicBoolean shuttingDown = new AtomicBoolean(false);

        try {
            // Create the Wikimedia event handler
            BackgroundEventHandler eventHandler =
                    new WikimediaChangeHandler(producer, topic);

            // Configure the Wikimedia connection and User-Agent
            EventSource.Builder eventSourceBuilder =
                    new EventSource.Builder(
                            ConnectStrategy.http(URI.create(url))
                                    .header(
                                            "User-Agent",
                                            "KafkaWikimediaDemo/1.0 (your-email@example.com)"
                                    )
                    );

            // Attach the handler to the background event source
            try (BackgroundEventSource eventSource =
                         new BackgroundEventSource.Builder(
                                 eventHandler,
                                 eventSourceBuilder
                         ).build()) {

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (shuttingDown.compareAndSet(false, true)) {
                        log.info("Shutdown requested");
                        eventSource.close();
                        producer.close();
                    }
                }));

                log.info("Starting Wikimedia recent changes stream");

                eventSource.start();

                // Run for 10 minutes
                TimeUnit.MINUTES.sleep(10);

                log.info("10 minutes elapsed. Stopping the stream.");
            }

        } finally {
            if (shuttingDown.compareAndSet(false, true)) {
                producer.close();
            }

            log.info("Wikimedia producer stopped");
        }
    }
}