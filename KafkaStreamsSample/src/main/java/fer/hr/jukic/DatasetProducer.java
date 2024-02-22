package fer.hr.jukic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class DatasetProducer {

    private static final long serialVersionUID = 1L;
    private static Map<Integer, Record> toSend = new ConcurrentHashMap<>();

    private static Random randomGenerator = new Random();
    private Integer idx = 0;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:29092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "InputTopic";
        String line;
        long n = 0;
        //just 4 random numbers in order to achieve log generator
        List<String> phoneNumbers = new ArrayList<String>();
        phoneNumbers.add("124989054575");
        phoneNumbers.add("122919054555");
        phoneNumbers.add("313999054777");
        phoneNumbers.add("421979054757");
        for(int i = 0; i < 1000; i++) {
            String fromNumber = phoneNumbers.get(randomGenerator.nextInt(4));
            String toNumber = phoneNumbers.get(randomGenerator.nextInt(4));

            while (fromNumber == toNumber) {
                toNumber = phoneNumbers.get(randomGenerator.nextInt(4));
            }

            Integer duration = randomGenerator.nextInt(120);
            toSend.put(i, new Record(new String[]{Long.toString(System.currentTimeMillis()), generateRandomUid(), fromNumber, toNumber, duration.toString()}));
        }


        for(Map.Entry<Integer,Record> entry: toSend.entrySet()){
            Integer key = entry.getValue().hashCode();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(key), entry.getValue().toString());
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Message sent successfully! Metadata: " + metadata);
                } else {
                    System.err.println("Error sending message: " + exception.getMessage());
                }
            });
            //implement Thread.sleep() for long period testing.
            n++;
        }

        System.out.println("Added: " + n + " rows");
        producer.flush();
        producer.close();
    }


    //it works for the examples
    public static String generateRandomUid() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString();
    }
}