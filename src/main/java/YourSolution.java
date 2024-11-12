import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class YourSolution {

    private static final String BASIC_TOPIC = Context.BASIC_TOPIC;
    private static final String TRANSACTIONAL_TOPIC = Context.TRANSACTIONAL_TOPIC;
    private static final String COMPACTING_TOPIC = Context.COMPACTING_TOPIC;



    public static void main(String[] args) {

        System.out.println("Running My Solution...");

        Properties kafkaProps = Context.getKafkaConnectionProperties();

        printNumberOfMessages(kafkaProps);
    }

    private static void printNumberOfMessages(Properties kafkaProps){
        // Admin to manage Kafka topics
        try (Admin adminClient = AdminClient.create(kafkaProps)) {
            printBasicTopicMessageCount(adminClient,BASIC_TOPIC,createConsumer(kafkaProps));
            printTransactionalTopicMessageCount(adminClient,TRANSACTIONAL_TOPIC,createConsumer(kafkaProps));
            printCompactionTopicMessageCount(adminClient,COMPACTING_TOPIC,createConsumer(kafkaProps));
        }

    }

    protected static void printBasicTopicMessageCount(Admin adminClient, String topic, Consumer<String,String> consumer){
        try {

            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(0);

            Map<TopicPartition, Long> endOffsetMap =  consumer.endOffsets(consumer.assignment());

            long totalMessagesInTopic = endOffsetMap.values().stream().mapToLong(Long::longValue).sum();
            System.out.println("Total Messages in topic : "+ topic +" is : "+totalMessagesInTopic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static void printTransactionalTopicMessageCount(Admin adminClient, String topic, Consumer<String,String> consumer){
        try {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(0);

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
            System.out.println("Total committed Messages in topic : "+ topic +" is :"+committedMsgs);
            System.out.println("Total uncommited Messages in topic : "+ topic +" is :"+unCommitedMsgs);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void printCompactionTopicMessageCount(Admin adminClient, String topic, Consumer<String,String> consumer){

        try {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(0);

            Map<TopicPartition, Long> endOffsetsMap = consumer.endOffsets(consumer.assignment());
            long totalCompatedMessages = endOffsetsMap.values().stream().mapToLong(Long::longValue).sum();
            System.out.println("Total Messages in topic : "+ topic +" is : "+totalCompatedMessages);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private static Properties getConsumerProps(Properties kafkaProps){
        Properties consumerProps = new Properties();
        consumerProps.putAll(kafkaProps);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group1");

        return consumerProps;
    }

    private static Consumer<String, String> createConsumer(Properties kafkaProps){
        return new KafkaConsumer<>(getConsumerProps(kafkaProps));
    }

}
