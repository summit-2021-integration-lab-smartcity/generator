package org.smartcity.generator;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

@ApplicationScoped
public class Generator {

    @ConfigProperty(name = "bucket.name")
    String bucketName;

    @ConfigProperty(name = "interval.seconds")
    int intervalSeconds;

    Random r = new Random();

    @Inject
    S3Client s3;

    @Outgoing("images")
    public Multi<Message<String>> generate() {
        return Multi.createFrom().ticks().every(Duration.ofSeconds(intervalSeconds)).onOverflow().drop()
                .map(tick -> toMessage(getRandomImage()));
    }

    private Message<String> toMessage(String image) {
        JsonObject message = new JsonObject().put("image", image).put("timestamp", Instant.now().toEpochMilli());
        return KafkaRecord.of(image, message.toString());
    }

    private String getRandomImage() {
        //read the files from the bucket
        ListObjectsRequest listRequest = ListObjectsRequest.builder().bucket(bucketName).build();
        List<String> images = s3.listObjects(listRequest).contents().stream().sorted(Comparator.comparing(S3Object::key))
                .map(S3Object::key).collect(Collectors.toList());
        int index = r.nextInt(images.size());
        return images.get(index);
    }

}
