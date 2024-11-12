import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

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

       Map<TopicPartition, Long> mockEndOffsetMap = Map.of(partition,300L);
       Mockito.when(mockConsumer.endOffsets(any())).thenReturn(mockEndOffsetMap);

       Map<TopicPartition, OffsetAndMetadata> mockCommitedOffsetMap = Map.of(partition, new OffsetAndMetadata(100L));
       Mockito.when(mockConsumer.committed((Set<TopicPartition>) any())).thenReturn(mockCommitedOffsetMap);

       //YourSolution.printTransactionalTopicMessageCount(mockAdmin,MOCK_TRANSACTIONAL_TOPIC,mockConsumer);
       //verify(mockConsumer,times(1)).endOffsets(any());

       TransactionalMsgResult transactionalMsgResult = YourSolution.printTransactionalTopicMessageCount(mockAdmin,MOCK_TRANSACTIONAL_TOPIC,mockConsumer);
       assertEquals(100L,transactionalMsgResult.getCommitedMessages());
       assertEquals(200L,transactionalMsgResult.getUnCommittedMessages());
   }

    @Test
    // Only latest message for unique key retained, older messages for this key compacted
    public void testPrintCompactingTopicMessageCount(){
        /*
        Not sure how to verify the compacting behavior, since framework has the control of compaction. --> check with Tom
         */
        Map<TopicPartition, Long> mockEndOffsetMap = Map.of(new TopicPartition(MOCK_COMPACTING_TOPIC,0),100L);

        Mockito.when(mockConsumer.endOffsets(any())).thenReturn(mockEndOffsetMap);

        MsgResult msgResult = YourSolution.printBasicTopicMessageCount(mockAdmin,MOCK_COMPACTING_TOPIC,mockConsumer);
        verify(mockConsumer,times(1)).endOffsets(any());

        assertEquals(100L,msgResult.getTotalMessages());
    }
}