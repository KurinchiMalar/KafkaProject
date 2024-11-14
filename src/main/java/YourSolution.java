import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class YourSolution {

    private static final String BASIC_TOPIC = Context.BASIC_TOPIC;
    private static final String TRANSACTIONAL_TOPIC = Context.TRANSACTIONAL_TOPIC;
    private static final String COMPACTING_TOPIC = Context.COMPACTING_TOPIC;


    // Exception handling --> InterruptedException  / ExecutionException / KafkaException to be handled
    // I am keeping it as Uber level Exception for brevity
    public static void main(String[] args) {

        System.out.println("Running My Solution...");

        Properties kafkaProps = Context.getKafkaConnectionProperties();
        // Do check KafkaProps is null throw exception , ignoring the same as of now.
        printNumberOfMessages(kafkaProps);
    }

    private static void printNumberOfMessages(Properties kafkaProps){
        // Admin to manage Kafka topics
        try (Admin adminClient = AdminClient.create(kafkaProps)) {

            Consumer<String,String> basicConsumer = KafkaConsumerFactory.createConsumer(kafkaProps,"basic-consumer-group",BASIC_TOPIC);
            Consumer<String,String> transactionalConsumer = KafkaConsumerFactory.createConsumer(kafkaProps,"transactional-consumer-group",TRANSACTIONAL_TOPIC);
            Consumer<String,String> compactingConsumer = KafkaConsumerFactory.createConsumer(kafkaProps,"compacting-consumer-group",COMPACTING_TOPIC);

            printBasicTopicMessageCount(adminClient,BASIC_TOPIC,basicConsumer);
            printTransactionalTopicMessageCount(adminClient,TRANSACTIONAL_TOPIC,transactionalConsumer);
            printCompactingTopicMessageCount(adminClient,COMPACTING_TOPIC,compactingConsumer);
        }

    }

    protected static MsgResult printBasicTopicMessageCount(Admin adminClient, String topic, Consumer<String,String> consumer){
        long totalMessagesInTopic = 0L;
        try {

            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(0);

            Map<TopicPartition, Long> endOffsetMap =  consumer.endOffsets(consumer.assignment());

            totalMessagesInTopic = endOffsetMap.values().stream().mapToLong(Long::longValue).sum();
            System.out.println("Total Messages in topic : "+ topic +" is : "+totalMessagesInTopic);
            // Commit offsets asynchronously
            consumer.commitAsync();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new MsgResult(totalMessagesInTopic);

    }

    protected static TransactionalMsgResult printTransactionalTopicMessageCount(Admin adminClient, String topic, Consumer<String,String> consumer){
        try {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Consumer subscribed to topic "+topic);

            //consumer.poll(Duration.ofMillis(100));
            while(consumer.assignment().isEmpty()){
                System.out.println("Still polling..."+consumer.toString()+" assignments: "+consumer.assignment());
                consumer.poll(Duration.ofMillis(100));
            }
            System.out.println("Consumer assigned partitions :" + consumer.assignment());
            long committedMsgs = 0;
            long unCommitedMsgs = 0;

            // consumer.assignment() is 0 , during debug . Let's poll until we have something assigned for this consumer.
            Map<TopicPartition, Long> endOffsetMap =consumer.endOffsets(consumer.assignment());
            Map<TopicPartition, OffsetAndMetadata> committedOffSetMap = consumer.committed(consumer.assignment());

            for(TopicPartition partition:endOffsetMap.keySet()){
                long curPartitionEndOffset = endOffsetMap.get(partition);
                // if nothing is committed, return a default value 0, else return the last successful commited offset
                OffsetAndMetadata committedOffsetMetaData = committedOffSetMap.get(partition);
                long curPartitionCommitedOffset = (committedOffsetMetaData == null) ? 0 :committedOffsetMetaData.offset();

                //Update Commited Messages Count
                committedMsgs += curPartitionCommitedOffset;

                //Rest of messages other than committedMsgs correspond to UncommitedMessages
                unCommitedMsgs += (curPartitionEndOffset - curPartitionCommitedOffset);

            }
            System.out.println("Total committed Messages in topic : "+ topic +" is :"+committedMsgs);
            System.out.println("Total uncommited Messages in topic : "+ topic +" is :"+unCommitedMsgs);
            // Commit offsets asynchronously
            consumer.commitAsync();
            return new TransactionalMsgResult(committedMsgs,unCommitedMsgs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static MsgResult printCompactingTopicMessageCount(Admin adminClient, String topic, Consumer<String,String> consumer){

            consumer.subscribe(Collections.singletonList(topic));
            Set<String> uniqueKeys = new HashSet<>(); // Set to track distinct keys

            try {
                    // Poll for up to 10s for new messages
                var records = consumer.poll(10000);

                    // Process each record
                for (ConsumerRecord<String, String> record : records) {
                    // Add the key to the set (only distinct keys will be counted)
                    uniqueKeys.add(record.key());
                }
                int totalMessagesAfterCompaction = uniqueKeys.size();
                // Print the total number of distinct messages (keys) after processing records
                System.out.println("Total number of messages (distinct keys): " + totalMessagesAfterCompaction);

                // Commit offsets asynchronously
                consumer.commitAsync();

                return new MsgResult(totalMessagesAfterCompaction);
            } finally {
                consumer.close();
            }
    }

}
