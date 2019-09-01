package schemakeeper.kafka;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DefaultNamingStrategyTest {
    private NamingStrategy strategy = new DefaultNamingStrategy();

    @Test
    public void keySchema() {
        assertEquals(strategy.resolveSubjectName("topic", true), "topic-key");
    }

    @Test
    public void valueSchema() {
        assertEquals(strategy.resolveSubjectName("topic", false), "topic-value");
    }
}
