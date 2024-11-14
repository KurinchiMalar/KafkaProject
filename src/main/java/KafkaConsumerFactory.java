import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

public class KafkaConsumerFactory {

    public static Consumer<String, String> createConsumer(Properties kafkaProps, String groupId, String topicType){
        // Validation for topicType
        List<String> allowedTopicTypes = List.of(Context.BASIC_TOPIC,Context.TRANSACTIONAL_TOPIC,Context.COMPACTING_TOPIC);
        if(!allowedTopicTypes.contains(topicType)) throw new IllegalArgumentException("Invalid topic type : "+topicType);

        Properties consumerProps = new Properties();
        consumerProps.putAll(kafkaProps);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        if(topicType.equalsIgnoreCase(Context.TRANSACTIONAL_TOPIC)){
            consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        }
        return new KafkaConsumer<>(consumerProps);
    }

}
