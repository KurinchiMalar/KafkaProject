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
            System.out.println("TOPIC : "+topic +" Total Messages : "+totalMessagesInTopic);
            System.out.println("-----------------------------------------------------");
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

            while(consumer.assignment().isEmpty()){
                System.out.println("Still polling..."+consumer.toString()+" assignments: "+consumer.assignment());
                consumer.poll(Duration.ofMillis(100));
            }
            System.out.println("Consumer assigned partitions :" + consumer.assignment());
            long committedMsgs = 0;
            long unCommitedMsgs = 0;

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
            System.out.println("TOPIC : "+topic);

            System.out.println("Total committed Messages : "+committedMsgs);
            System.out.println("Total uncommited Messages : "+unCommitedMsgs);
            System.out.println("-----------------------------------------------------");
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
                var records = consumer.poll(10000); // 10s polling

                for (ConsumerRecord<String, String> record : records) {
                    uniqueKeys.add(record.key());
                }
                int totalMessagesAfterCompaction = uniqueKeys.size();

                System.out.println("TOPIC : "+topic);
                System.out.println("Total number of messages (distinct keys): " + totalMessagesAfterCompaction);
                System.out.println("-----------------------------------------------------");

                // Commit offsets asynchronously
                consumer.commitAsync();

                return new MsgResult(totalMessagesAfterCompaction);
            } finally {
                consumer.close();
            }
    }

}
