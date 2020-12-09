import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {

    private static final String VALID_TRANSACTIONS_TOPIC = "valid-transactions";
    private static final String SUSPICIOUS_TRANSACTIONS_TOPIC = "suspicious-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws FileNotFoundException {
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader();
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase();
        Producer<String, Transaction> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);
        Application application = new Application();

        try {
            application.processTransactions(incomingTransactionsReader, customerAddressDatabase, kafkaProducer);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }

    public static Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "bank-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        return new KafkaProducer<String, Transaction>(properties);
    }

    public void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                    CustomerAddressDatabase customerAddressDatabase,
                                    Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException, FileNotFoundException {
        // Retrieve the next transaction from the IncomingTransactionsReader
        // For the transaction user, get the user residence from the UserResidenceDatabase
        // Compare user residence to transaction location.
        // Send a message to the appropriate topic, depending on whether the user residence and transaction
        // location match or not.
        // Print record metadata information

        File file = new File("C:\\Users\\William\\Documents\\4th Year\\Distributed Systems\\Kafka Assignment (35%)\\kafka-assignment-WilliamVida\\bank-api-service\\src\\main\\resources\\user-transactions.txt");
        Scanner scanner = new Scanner(file);

        Transaction[] validTransactions = new Transaction[100];
        Transaction[] suspiciousTransactions = new Transaction[100];

        int validCount = 0;
        int suspiciousCount = 0;

        while (incomingTransactionsReader.hasNext()) {
            String[] transactions = scanner.nextLine().split(" ");
            String user = transactions[0];
            String transactionLocation = transactions[1];
            double amount = Double.parseDouble(transactions[2]);

            if (!transactionLocation.equalsIgnoreCase(customerAddressDatabase.getUserResidence(incomingTransactionsReader.next().getUser()))) {
                suspiciousTransactions[suspiciousCount] = new Transaction(user, amount, transactionLocation);
                ProducerRecord<String, Transaction> suspiciousRecord = new ProducerRecord<>(SUSPICIOUS_TRANSACTIONS_TOPIC, suspiciousTransactions[suspiciousCount]);
                RecordMetadata recordMetadata = kafkaProducer.send(suspiciousRecord).get();
                System.out.println(String.format("Record with (key: %s, value: %s), was sent to (partition: %d, offset: %d, topic: %s).\n", suspiciousRecord.value().getUser(), suspiciousRecord.value(), recordMetadata.partition(), recordMetadata.offset(), suspiciousRecord.topic()));
                suspiciousCount++;
            } else {
                validTransactions[validCount] = new Transaction(user, amount, transactionLocation);
                ProducerRecord<String, Transaction> validRecord = new ProducerRecord<>(VALID_TRANSACTIONS_TOPIC, validTransactions[validCount]);
                RecordMetadata recordMetadata = kafkaProducer.send(validRecord).get();
                System.out.println(String.format("Record with (key: %s, value: %s), was sent to (partition: %d, offset: %d, topic: %s).\n", validRecord.value().getUser(), validRecord.value(), recordMetadata.partition(), recordMetadata.offset(), validRecord.topic()));
                validCount++;
            }
        }
    }

}
