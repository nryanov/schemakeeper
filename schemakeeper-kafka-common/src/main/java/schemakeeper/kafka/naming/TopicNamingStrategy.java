package schemakeeper.kafka.naming;

import org.apache.avro.Schema;

/**
 *
 */
public class TopicNamingStrategy implements NamingStrategy {
    public static NamingStrategy INSTANCE = new TopicNamingStrategy();

    @Override
    public String resolveSubjectName(String topicName, boolean isKey, Schema schema) {
        if (isKey) {
            return topicName + "-key";
        } else {
            return topicName + "-value";
        }
    }
}
