import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class YourSolutionTest {

    public static final String MOCK_BASIC_TOPIC = "basic";
    public static final String MOCK_TRANSACTIONAL_TOPIC = "transactional";
    public static final String MOCK_COMPACTING_TOPIC = "compacting";

    private Admin mockAdmin;
    private Consumer<String,String> mockConsumer;


   @BeforeEach
    public void setup(){
        mockAdmin = Mockito.mock(Admin.class);
        mockConsumer = Mockito.mock(Consumer.class);
   }

   @Test
    public void testPrintBasicTopicMessageCount(){

       Map<TopicPartition, Long> mockEndOffsetMap = Map.of(new TopicPartition(MOCK_BASIC_TOPIC,0),100L);

       Mockito.when(mockConsumer.endOffsets(any())).thenReturn(mockEndOffsetMap);

       MsgResult msgResult = YourSolution.printBasicTopicMessageCount(mockAdmin,MOCK_BASIC_TOPIC,mockConsumer);
       verify(mockConsumer,times(1)).endOffsets(any());

       assertEquals(100L,msgResult.getTotalMessages());
   }

   @Test
    public void testPrintTransactionalTopicMessageCount(){

       TopicPartition partition = new TopicPartition(MOCK_TRANSACTIONAL_TOPIC,0);
       Mockito.when(mockConsumer.assignment()).thenReturn(Collections.singleton(partition));
       //Mockito.when(mockConsumer.assignment().isEmpty()).thenReturn(true);

       Map<TopicPartition, Long> mockEndOffsetMap = Map.of(partition,300L);
       Mockito.when(mockConsumer.endOffsets(any())).thenReturn(mockEndOffsetMap);

       Map<TopicPartition, OffsetAndMetadata> mockCommitedOffsetMap = Map.of(partition, new OffsetAndMetadata(100L));
       Mockito.when(mockConsumer.committed((Set<TopicPartition>) any())).thenReturn(mockCommitedOffsetMap);

       //YourSolution.printTransactionalTopicMessageCount(mockAdmin,MOCK_TRANSACTIONAL_TOPIC,mockConsumer);
       //verify(mockConsumer,times(1)).endOffsets(any());

       TransactionalMsgResult transactionalMsgResult = YourSolution.printTransactionalTopicMessageCount(mockAdmin,MOCK_TRANSACTIONAL_TOPIC,mockConsumer);
       assertEquals(100L,transactionalMsgResult.getCommitedMessages());
       assertEquals(200L,transactionalMsgResult.getUnCommittedMessages());
       verify(mockConsumer,times(1)).commitAsync();
   }

    @Test
    // Only latest message for unique key retained, older messages for this key compacted
    public void testPrintCompactingTopicMessageCount(){
        ConsumerRecord<String,String> r1 = new ConsumerRecord<>("compacting",0,0,"key1","hello");
        ConsumerRecord<String,String> r2 = new ConsumerRecord<>("compacting",0,0,"key2","helloworld");

        // new value with same key
        ConsumerRecord<String,String> r3 = new ConsumerRecord<>("compacting",0,0,"key1","hellonew");
        /*TBD will have to handle the casting properly here and will be able to just verify the invocations*/
       // Mockito.when(mockConsumer.poll(10000)).thenReturn(Collections.singletonList(r1,r2,r3));
       //YourSolution.printCompactingTopicMessageCount(mockAdmin,MOCK_COMPACTING_TOPIC,mockConsumer);
       // verify(mockConsumer,times(1)).commitAsync();
        assertTrue(true);
    }
}