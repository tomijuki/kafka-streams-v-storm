package fer.hr.jukic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class CallCountStream{

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "call-count-streams-sample");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        config.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        config.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> callStream = builder.stream("InputTopic", Consumed.with(Serdes.String(), Serdes.String()));
        callStream.print(Printed.toSysOut());

        ObjectMapper objectMapper = new ObjectMapper();

        KTable<String, Long> callCounts = callStream
                .flatMap((key, record) -> {
                    // Extracting from and to numbers from the record
                    JsonNode jsonNode = null;
                    try {
                        jsonNode = objectMapper.readTree(record);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }

                    String fromNumber = jsonNode.get("fromNumber").asText();
                    String toNumber = jsonNode.get("toNumber").asText();
                    String keyString = fromNumber + "-" + toNumber;

                    return Arrays.asList(
                            new KeyValue<>(keyString, 1)
                    );
                })
                .groupByKey()
                .count(Named.as("Counts"));

        callCounts.toStream().print(Printed.toSysOut());
        callCounts.toStream().to("OutputTopic", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Add shutdown hook to close the streams application gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        while (true) {
            System.out.println(streams.toString());
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}