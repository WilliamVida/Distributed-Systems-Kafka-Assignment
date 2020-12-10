import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Application {

    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        Application kafkaConsumerApp = new Application();
        String consumerGroup = "reporting-service";
        List<String> topicList = Arrays.asList(VALID_TRANSACTIONS_TOPIC, SUSPICIOUS_TRANSACTIONS_TOPIC);

        if (args.length == 1) {
            consumerGroup = args[0];
        }

        System.out.println("Consumer is part of consumer group " + consumerGroup + ".\n");

        Consumer<String, Transaction> kafkaConsumer = kafkaConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);

        kafkaConsumerApp.consumeMessages(topicList, kafkaConsumer);
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        while (true) {
            kafkaConsumer.subscribe(Collections.singletonList(topics.get(0)));
            ConsumerRecords<String, Transaction> validConsumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if (validConsumerRecords.isEmpty()) {

            }

            for (ConsumerRecord<String, Transaction> record : validConsumerRecords) {
                System.out.println(String.format("Received record (key: %s, value: %s), partition: %d, offset: %d.", record.value().getUser(), record.value(), record.partition(), record.offset(), record.topic()));
                recordTransactionForReporting(record.topic(), record.value());
            }

            kafkaConsumer.commitAsync();

            kafkaConsumer.subscribe(Collections.singletonList(topics.get(1)));
            ConsumerRecords<String, Transaction> suspiciousConsumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if (suspiciousConsumerRecords.isEmpty()) {

            }

            for (ConsumerRecord<String, Transaction> record : suspiciousConsumerRecords) {
                System.out.println(String.format("Received record (key: %s, value: %s), partition: %d, offset: %d.", record.value().getUser(), record.value(), record.partition(), record.offset(), record.topic()));
                recordTransactionForReporting(record.topic(), record.value());
            }

            kafkaConsumer.commitAsync();
        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(properties);
    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print transaction information to the console
        // Print a different message depending on whether transaction is suspicious or valid

        if (topic.equalsIgnoreCase(SUSPICIOUS_TRANSACTIONS_TOPIC)) {
            System.out.println("Recording suspicious transaction for user " + transaction.getUser() + ", amount " + transaction.getAmount() + " originating in " + transaction.getTransactionLocation() + " for further investigation.\n");
        } else if (topic.equalsIgnoreCase(VALID_TRANSACTIONS_TOPIC)) {
            System.out.println("Recording transaction for user " + transaction.getUser() + ", amount " + transaction.getAmount() + " to show on user's monthly statement.\n");
        }
    }

}
