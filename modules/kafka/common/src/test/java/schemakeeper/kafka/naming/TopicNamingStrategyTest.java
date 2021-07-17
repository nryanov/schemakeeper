package schemakeeper.kafka.naming;

import org.junit.Test;
import schemakeeper.kafka.naming.TopicNamingStrategy;
import schemakeeper.kafka.naming.NamingStrategy;

import static org.junit.Assert.*;

public class TopicNamingStrategyTest {
    private NamingStrategy strategy = new TopicNamingStrategy();

    @Test
    public void keySchema() {
        assertEquals(strategy.resolveSubjectName("topic", true, null), "topic-key");
    }

    @Test
    public void valueSchema() {
        assertEquals(strategy.resolveSubjectName("topic", false, null), "topic-value");
    }
}
