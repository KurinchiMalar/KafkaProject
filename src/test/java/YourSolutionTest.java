import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class YourSolutionTest {

    private Admin mockAdmin;
    private Consumer<String,String> mockConsumer;


   @BeforeEach
    public void setup(){
        mockAdmin = Mockito.mock(Admin.class);
        mockConsumer = Mockito.mock(Consumer.class);
   }

   @Test
    public void testPrintBasicTopicMessageCount(){

       Map<TopicPartition, Long> mockEndOffsetMap = Map.of(new TopicPartition("mock_basic_topic",0),100L);

       Mockito.when(mockConsumer.endOffsets(any())).thenReturn(mockEndOffsetMap);

       YourSolution.printBasicTopicMessageCount(mockAdmin,"mock_basic_topic",mockConsumer);
       verify(mockConsumer,times(1)).endOffsets(any());
   }
}